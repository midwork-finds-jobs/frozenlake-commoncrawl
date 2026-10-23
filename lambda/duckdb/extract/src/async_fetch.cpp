// Non-blocking range requests for WARC records with libcurl's multi interface.
//
// One thread drives every transfer of the process, so hundreds of requests are in flight without a thread for each
// of them and connections are reused across records. S3 requests are signed with AWS Signature Version 4 using the
// credentials of the S3 secret which DuckDB would use for the path. A request which hasn't finished in hedge_ms gets
// a second identical request and the first one to finish wins, the other one is cancelled. Throttling, server
// errors and connection problems are retried.

#include "extract_extension.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <mutex>
#include <random>
#include <thread>
#include <unistd.h>
#include <algorithm>

namespace duckdb {

namespace {

using Clock = std::chrono::steady_clock;

string Hex(const unsigned char *data, idx_t size) {
	static const char *HEX = "0123456789abcdef";
	string out;
	for (idx_t i = 0; i < size; i++) {
		out.push_back(HEX[data[i] >> 4]);
		out.push_back(HEX[data[i] & 15]);
	}
	return out;
}

string Sha256Hex(const string &data) {
	unsigned char digest[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(data.data()), data.size(), digest);
	return Hex(digest, SHA256_DIGEST_LENGTH);
}

string HmacSha256(const string &key, const string &data) {
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int length = 0;
	HMAC(EVP_sha256(), key.data(), int(key.size()), reinterpret_cast<const unsigned char *>(data.data()), data.size(),
	     digest, &length);
	return string(reinterpret_cast<char *>(digest), length);
}

//! S3 object keys are signed URI encoded, slashes stay as they are
string UriEncode(const string &value) {
	static const char *HEX = "0123456789ABCDEF";
	string out;
	for (auto c : value) {
		if (isalnum(uint8_t(c)) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/') {
			out.push_back(c);
		} else {
			out.push_back('%');
			out.push_back(HEX[uint8_t(c) >> 4]);
			out.push_back(HEX[uint8_t(c) & 15]);
		}
	}
	return out;
}

//! CA bundles of the common Linux distributions and macOS
string CaBundle() {
	for (auto path : {"/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt",
	                  "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", "/etc/ssl/ca-bundle.pem", "/etc/ssl/cert.pem"}) {
		if (access(path, R_OK) == 0) {
			return path;
		}
	}
	return string();
}

//! The first certificate of a PEM bundle
string FirstPemCertificate(const string &path) {
	static const string END = "-----END CERTIFICATE-----";
	auto file = fopen(path.c_str(), "r");
	if (!file) {
		return string();
	}
	string content;
	char buffer[4096];
	size_t read;
	while ((read = fread(buffer, 1, sizeof(buffer), file)) > 0 && content.find(END) == string::npos) {
		content.append(buffer, read);
	}
	fclose(file);
	auto begin = content.find("-----BEGIN CERTIFICATE-----");
	auto end = content.find(END);
	if (begin == string::npos || end == string::npos) {
		return string();
	}
	return content.substr(begin, end + END.size() - begin) + "\n";
}

//! Replaces the CA store curl built for a connection with the one parsed once
CURLcode SetCaStore(CURL *, void *ssl_ctx, void *store) {
	SSL_CTX_set1_cert_store(static_cast<SSL_CTX *>(ssl_ctx), static_cast<X509_STORE *>(store));
	return CURLE_OK;
}

std::atomic<idx_t> request_count(0), request_retries(0), request_errors(0), slow_requests(0), max_request_ms(0),
    request_hedges(0);

std::mutex last_retry_lock;
string last_retry;
string slowest;
idx_t slowest_us = 0;

struct Transfer;

//! One request of the caller, done by one or two transfers
struct Request {
	AsyncRequest request;
	idx_t attempts = 0;
	//! When the request left the queue, waiting for a free transfer isn't slowness of S3
	Clock::time_point started;
	Clock::time_point retry_at;
	bool finished = false;
	bool hedged = false;
	vector<Transfer *> transfers;
};

struct Transfer {
	CURL *easy = nullptr;
	curl_slist *headers = nullptr;
	shared_ptr<Request> request;
	string data;
	//! Why WriteCallback() aborted the transfer, such errors aren't retried
	string failure;
	char error[CURL_ERROR_SIZE] = {0};
};

size_t WriteCallback(char *data, size_t size, size_t count, void *pointer) {
	auto &transfer = *static_cast<Transfer *>(pointer);
	auto bytes = size * count;
	auto length = transfer.request->request.length;
	// A server which ignores the Range header would send the whole file
	if (length > 0 && transfer.data.size() + bytes > NumericCast<idx_t>(length)) {
		transfer.failure = StringUtil::Format("the response is longer than the requested %lld bytes", length);
		return 0;
	}
	try {
		transfer.data.append(data, bytes);
	} catch (std::exception &) {
		// Exceptions must not unwind through curl's C code, returning less than bytes aborts the transfer
		transfer.failure = "out of memory";
		return 0;
	}
	return bytes;
}

class Fetcher {
public:
	static Fetcher &Get() {
		// Never destroyed: the thread runs until the process exits
		static auto fetcher = new Fetcher();
		return *fetcher;
	}

	void Submit(AsyncRequest request) {
		auto pending = make_shared_ptr<Request>();
		pending->request = std::move(request);
		{
			std::lock_guard<std::mutex> guard(lock);
			queue.push_back(std::move(pending));
		}
		curl_multi_wakeup(multi);
	}

	void SetConcurrency(idx_t value) {
		concurrency = MaxValue<idx_t>(1, value);
	}

	static constexpr idx_t MAX_ATTEMPTS = 5;
	static constexpr idx_t BASE_BACKOFF_MS = 1000;
	static constexpr idx_t MAX_BACKOFF_MS = 10000;
	//! Throttling never lowers the concurrency below this
	static constexpr double MIN_ALLOWED = 8;

private:
	CURLM *multi;
	CURLSH *share;
	string ca_bundle;
	//! The CA bundle parsed once. curl would parse it for every new connection: it caches it only without a CA
	//! directory and vcpkg builds it with /etc/ssl/certs, which took 30% of the CPU when opening 128 connections.
	X509_STORE *ca_store = nullptr;
	//! curl still builds a store for every connection before SetCaStore() replaces it, from this one certificate
	string placeholder_ca;
	std::mutex lock;
	std::deque<shared_ptr<Request>> queue;
	//! Owned by the fetcher thread
	vector<shared_ptr<Request>> retrying;
	vector<shared_ptr<Request>> running;
	std::atomic<idx_t> concurrency {256};
	//! Requests allowed at a time: lowered when S3 throttles (SlowDown), owned by the fetcher thread
	double allowed = 1e9;
	Clock::time_point last_throttle;
	std::mt19937_64 random {std::random_device()()};
	idx_t transfers = 0;

	//! Multiplicative decrease, at most once per 200ms because S3 throttles a burst of requests at once
	void Throttled() {
		auto now = Clock::now();
		if (now - last_throttle < std::chrono::milliseconds(200)) {
			return;
		}
		last_throttle = now;
		allowed = MaxValue(MIN_ALLOWED, MinValue<double>(allowed, double(transfers)) * 0.7);
	}

	Fetcher() {
		curl_global_init(CURL_GLOBAL_DEFAULT);
		multi = curl_multi_init();
		share = curl_share_init();
		curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
		curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
		curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS, long(1024));
		ca_bundle = CaBundle();
		placeholder_ca = ca_bundle.empty() ? string() : FirstPemCertificate(ca_bundle);
		if (!placeholder_ca.empty()) {
			ca_store = X509_STORE_new();
			if (ca_store && X509_STORE_load_file(ca_store, ca_bundle.c_str()) != 1) {
				X509_STORE_free(ca_store);
				ca_store = nullptr;
			}
		}
		std::thread([this]() { Run(); }).detach();
	}

	void Start(const shared_ptr<Request> &request) {
		auto transfer = new Transfer();
		transfer->request = request;
		transfer->easy = curl_easy_init();
		auto easy = transfer->easy;
		curl_easy_setopt(easy, CURLOPT_URL, request->request.url.c_str());
		for (auto &header : request->request.headers) {
			transfer->headers = curl_slist_append(transfer->headers, header.c_str());
		}
		curl_easy_setopt(easy, CURLOPT_HTTPHEADER, transfer->headers);
		curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(easy, CURLOPT_WRITEDATA, transfer);
		curl_easy_setopt(easy, CURLOPT_PRIVATE, transfer);
		curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, transfer->error);
		curl_easy_setopt(easy, CURLOPT_NOSIGNAL, long(1));
		curl_easy_setopt(easy, CURLOPT_SHARE, share);
		curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, long(10000));
		curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, long(60000));
		curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, long(1));
		curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, long(1));
		if (ca_store) {
			curl_blob blob {const_cast<char *>(placeholder_ca.data()), placeholder_ca.size(), CURL_BLOB_NOCOPY};
			curl_easy_setopt(easy, CURLOPT_CAINFO_BLOB, &blob);
			curl_easy_setopt(easy, CURLOPT_SSL_CTX_FUNCTION, SetCaStore);
			curl_easy_setopt(easy, CURLOPT_SSL_CTX_DATA, ca_store);
		} else if (!ca_bundle.empty()) {
			curl_easy_setopt(easy, CURLOPT_CAINFO, ca_bundle.c_str());
		}
		if (request->request.length > 0) {
			transfer->data.reserve(NumericCast<idx_t>(request->request.length));
		}
		request->transfers.push_back(transfer);
		curl_multi_add_handle(multi, easy);
		transfers++;
	}

	void Remove(Transfer *transfer) {
		auto &list = transfer->request->transfers;
		list.erase(std::remove(list.begin(), list.end(), transfer), list.end());
		curl_multi_remove_handle(multi, transfer->easy);
		curl_easy_cleanup(transfer->easy);
		curl_slist_free_all(transfer->headers);
		delete transfer;
		transfers--;
	}

	static bool Retryable(CURLcode code, long status) {
		switch (code) {
		case CURLE_OK:
			break;
		// Network problems, a bad URL or certificate would fail the same way again
		case CURLE_COULDNT_RESOLVE_HOST:
		case CURLE_COULDNT_CONNECT:
		case CURLE_OPERATION_TIMEDOUT:
		case CURLE_SEND_ERROR:
		case CURLE_RECV_ERROR:
		case CURLE_PARTIAL_FILE:
		case CURLE_GOT_NOTHING:
		case CURLE_SSL_CONNECT_ERROR:
		case CURLE_HTTP2:
		case CURLE_HTTP2_STREAM:
			return true;
		default:
			return false;
		}
		return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
	}

	void Finish(const shared_ptr<Request> &request, string data, string error) {
		request->finished = true;
		for (auto transfer : vector<Transfer *>(request->transfers)) {
			Remove(transfer);
		}
		auto elapsed =
		    idx_t(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - request->started).count());
		request_count++;
		if (!error.empty()) {
			request_errors++;
		}
		if (elapsed >= 1000) {
			slow_requests++;
		}
		for (auto max = max_request_ms.load(); elapsed > max && !max_request_ms.compare_exchange_weak(max, elapsed);) {
		}
		try {
			request->request.on_done(std::move(data), std::move(error));
		} catch (...) {
		}
	}

	//! Remembers where the slowest transfer since the last stats call spent its time
	static void RecordPhases(CURL *easy) {
		curl_off_t dns = 0, connect = 0, tls = 0, first_byte = 0, total = 0;
		curl_easy_getinfo(easy, CURLINFO_NAMELOOKUP_TIME_T, &dns);
		curl_easy_getinfo(easy, CURLINFO_CONNECT_TIME_T, &connect);
		curl_easy_getinfo(easy, CURLINFO_APPCONNECT_TIME_T, &tls);
		curl_easy_getinfo(easy, CURLINFO_STARTTRANSFER_TIME_T, &first_byte);
		curl_easy_getinfo(easy, CURLINFO_TOTAL_TIME_T, &total);
		std::lock_guard<std::mutex> guard(last_retry_lock);
		if (idx_t(total) > slowest_us) {
			slowest_us = idx_t(total);
			slowest = StringUtil::Format("dns=%lldms connect=%lldms tls=%lldms first_byte=%lldms total=%lldms",
			                             (long long)dns / 1000, (long long)connect / 1000, (long long)tls / 1000,
			                             (long long)first_byte / 1000, (long long)total / 1000);
		}
	}

	void Completed(Transfer *transfer, CURLcode code) {
		auto request = transfer->request;
		if (request->finished) {
			Remove(transfer);
			return;
		}
		long status = 0;
		curl_easy_getinfo(transfer->easy, CURLINFO_RESPONSE_CODE, &status);
		RecordPhases(transfer->easy);
		if (!transfer->failure.empty()) {
			auto failure = transfer->failure;
			Remove(transfer);
			if (request->transfers.empty()) {
				Finish(request, string(), failure);
			}
			return;
		}
		if (code == CURLE_OK && (status == 200 || status == 206)) {
			auto data = std::move(transfer->data);
			if (request->request.length > 0 &&
			    data.size() != NumericCast<idx_t>(request->request.length)) {
				Finish(request, string(),
				       StringUtil::Format("expected %lld bytes but got %llu", request->request.length, data.size()));
				return;
			}
			// Additive increase back to the configured concurrency after throttling
			allowed = MinValue<double>(double(concurrency), allowed + 0.25);
			Finish(request, std::move(data), string());
			return;
		}
		string error = code != CURLE_OK ? StringUtil::Format("%s: %s", curl_easy_strerror(code), transfer->error)
		                                : StringUtil::Format("HTTP %ld: %s", status, transfer->data.substr(0, 200));
		if (status == 429 || status == 503) {
			Throttled();
		}
		Remove(transfer);
		if (!request->transfers.empty()) {
			// The hedged transfer can still succeed
			return;
		}
		if (Retryable(code, status) && ++request->attempts <= MAX_ATTEMPTS) {
			request_retries++;
			{
				std::lock_guard<std::mutex> guard(last_retry_lock);
				last_retry = error;
			}
			// Common Crawl asks to wait at least 1s after a 503 and to back off to 10s: 1s, 2s, 4s, 8s, 10s plus up to
			// half of it as jitter. Without jitter the requests which S3 throttled together were retried together and
			// throttled again.
			auto backoff = MinValue<idx_t>(MAX_BACKOFF_MS, BASE_BACKOFF_MS << (request->attempts - 1));
			auto jitter = std::uniform_int_distribution<idx_t>(0, backoff / 2)(random);
			request->retry_at = Clock::now() + std::chrono::milliseconds(backoff + jitter);
			retrying.push_back(request);
			return;
		}
		Finish(request, string(), error);
	}

	void Run() {
		while (true) {
			// Every transfer counts against the limit, so that retries and hedges don't undo the throttling
			auto limit = MinValue<idx_t>(concurrency, idx_t(allowed));
			auto now = Clock::now();
			auto throttled = idx_t(allowed) < concurrency;
			for (idx_t i = 0; i < retrying.size();) {
				if (retrying[i]->retry_at <= now && transfers < limit) {
					retrying[i]->started = now;
					Start(retrying[i]);
					retrying.erase(retrying.begin() + i);
				} else {
					i++;
				}
			}
			vector<shared_ptr<Request>> starting;
			{
				// Started outside of the lock so that Submit() doesn't wait for curl's setup
				std::lock_guard<std::mutex> guard(lock);
				while (!queue.empty() && transfers + starting.size() < limit) {
					starting.push_back(std::move(queue.front()));
					queue.pop_front();
				}
			}
			for (auto &request : starting) {
				request->started = now;
				running.push_back(request);
				Start(request);
			}
			// Hedge slow requests
			for (auto &request : running) {
				auto hedge_ms = request->request.hedge_ms;
				// S3 is slow when it throttles, a second request would only add to it
				if (transfers < limit && !throttled && !request->finished && !request->hedged && hedge_ms > 0 &&
				    request->transfers.size() == 1 && now - request->started > std::chrono::milliseconds(hedge_ms)) {
					request->hedged = true;
					request_hedges++;
					Start(request);
				}
			}
			int active = 0;
			curl_multi_perform(multi, &active);
			CURLMsg *message;
			int left = 0;
			while ((message = curl_multi_info_read(multi, &left))) {
				if (message->msg != CURLMSG_DONE) {
					continue;
				}
				Transfer *transfer = nullptr;
				curl_easy_getinfo(message->easy_handle, CURLINFO_PRIVATE, reinterpret_cast<char **>(&transfer));
				Completed(transfer, message->data.result);
			}
			running.erase(std::remove_if(running.begin(), running.end(),
			                             [](const shared_ptr<Request> &request) { return request->finished; }),
			              running.end());
			curl_multi_poll(multi, nullptr, 0, 20, nullptr);
		}
	}
};

} // namespace

void SubmitAsyncRequest(AsyncRequest request) {
	Fetcher::Get().Submit(std::move(request));
}

void SetAsyncConcurrency(idx_t concurrency) {
	Fetcher::Get().SetConcurrency(concurrency);
}

AsyncRequestStats GetAsyncRequestStats() {
	AsyncRequestStats stats;
	stats.requests = request_count;
	stats.retries = request_retries;
	stats.errors = request_errors;
	stats.slow = slow_requests;
	stats.max_ms = max_request_ms.exchange(0);
	stats.hedges = request_hedges;
	std::lock_guard<std::mutex> guard(last_retry_lock);
	stats.last_retry = last_retry;
	stats.slowest = std::move(slowest);
	slowest.clear();
	slowest_us = 0;
	return stats;
}

bool S3Credentials::Lookup(ClientContext &context, const string &path, S3Credentials &credentials) {
	auto &secrets = SecretManager::Get(context);
	auto match = secrets.LookupSecret(CatalogTransaction::GetSystemCatalogTransaction(context), path, "s3");
	if (!match.HasMatch()) {
		return false;
	}
	auto &secret = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
	auto get = [&](const char *key) {
		Value value;
		return secret.TryGetValue(Identifier(key), value) && !value.IsNull() ? value.ToString() : string();
	};
	credentials.key_id = get("key_id");
	credentials.secret = get("secret");
	credentials.session_token = get("session_token");
	credentials.region = get("region");
	if (credentials.region.empty()) {
		credentials.region = "us-east-1";
	}
	credentials.endpoint = get("endpoint");
	return !credentials.key_id.empty();
}

AsyncRequest MakeRangeRequest(const string &path, int64_t offset, int64_t length,
                              optional_ptr<const S3Credentials> credentials) {
	AsyncRequest request;
	request.offset = offset;
	request.length = length;
	request.headers.push_back(StringUtil::Format("Range: bytes=%lld-%lld", offset, offset + length - 1));
	if (!StringUtil::StartsWith(path, "s3://")) {
		request.url = path;
		return request;
	}
	auto rest = path.substr(5);
	auto slash = rest.find('/');
	auto bucket = rest.substr(0, slash);
	auto key = slash == string::npos ? string() : rest.substr(slash + 1);
	string host;
	if (credentials && !credentials->endpoint.empty() && credentials->endpoint != "s3.amazonaws.com") {
		host = bucket + "." + credentials->endpoint;
	} else {
		auto region = credentials ? credentials->region : string("us-east-1");
		host = region == "us-east-1" ? bucket + ".s3.amazonaws.com" : bucket + ".s3." + region + ".amazonaws.com";
	}
	auto uri = "/" + UriEncode(key);
	request.url = "https://" + host + uri;
	if (!credentials) {
		return request;
	}

	// AWS Signature Version 4 without a payload
	auto now = std::time(nullptr);
	std::tm utc;
	gmtime_r(&now, &utc);
	char amz_date[17], date[9];
	strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &utc);
	strftime(date, sizeof(date), "%Y%m%d", &utc);
	string canonical_headers = "host:" + host + "\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:" + amz_date + "\n";
	string signed_headers = "host;x-amz-content-sha256;x-amz-date";
	if (!credentials->session_token.empty()) {
		canonical_headers += "x-amz-security-token:" + credentials->session_token + "\n";
		signed_headers += ";x-amz-security-token";
	}
	auto canonical_request =
	    "GET\n" + uri + "\n\n" + canonical_headers + "\n" + signed_headers + "\nUNSIGNED-PAYLOAD";
	auto scope = string(date) + "/" + credentials->region + "/s3/aws4_request";
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + string(amz_date) + "\n" + scope + "\n" + Sha256Hex(canonical_request);
	auto signing_key = HmacSha256(
	    HmacSha256(HmacSha256(HmacSha256("AWS4" + credentials->secret, date), credentials->region), "s3"),
	    "aws4_request");
	auto signature = HmacSha256(signing_key, string_to_sign);
	request.headers.push_back("x-amz-content-sha256: UNSIGNED-PAYLOAD");
	request.headers.push_back(string("x-amz-date: ") + amz_date);
	if (!credentials->session_token.empty()) {
		request.headers.push_back("x-amz-security-token: " + credentials->session_token);
	}
	request.headers.push_back("Authorization: AWS4-HMAC-SHA256 Credential=" + credentials->key_id + "/" + scope +
	                          ", SignedHeaders=" + signed_headers +
	                          ", Signature=" + Hex(reinterpret_cast<const unsigned char *>(signature.data()),
	                                               signature.size()));
	return request;
}

} // namespace duckdb

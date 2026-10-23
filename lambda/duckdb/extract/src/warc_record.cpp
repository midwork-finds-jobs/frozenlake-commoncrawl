// warc_record(filename, offset, length[, charset]) -> STRUCT(status INTEGER, headers MAP(VARCHAR, VARCHAR),
//     body VARCHAR, charset VARCHAR, warc MAP(VARCHAR, VARCHAR), error VARCHAR)
//
// Fetches one gzip member of a Common Crawl WARC file and parses the WARC and HTTP headers. Reads go through DuckDB's
// own FileSystem so httpfs, S3 secrets and allowed_directories apply. Small S3 range requests are latency bound so the
// records are fetched by a process wide pool of `warc_concurrency` threads. Problems with single records are returned
// in the error field instead of failing the query.

#include "extract_extension.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "miniz_wrapper.hpp"

#include <libdeflate.h>
#include <zlib.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>

namespace duckdb {

namespace {

//! Common Crawl records are at most a few megabytes, these limits protect the lambda from unexpected input
constexpr idx_t MAX_COMPRESSED_BYTES = 16 * 1024 * 1024;
constexpr idx_t MAX_RECORD_BYTES = 64 * 1024 * 1024;

//! Counters of the fetches of this process for warc_fetch_stats(), the lambda reports their change per query
std::atomic<idx_t> fetch_count(0), fetch_retries(0), fetch_errors(0), slow_fetches(0), max_fetch_ms(0), fetch_hedges(0);

//! One range read which can be done by two requests at the same time, the first one to finish wins
struct RangeRead {
	std::mutex lock;
	std::condition_variable finished;
	bool done = false;
	//! Requests which are still running
	idx_t running = 0;
	string data;
	string error;
};

OpenFileInfo RecordFileInfo(const string &path);

//! Reads the byte range with retries and gives the result to `read` unless another request was faster
void ReadRange(FileSystem &fs, const string &path, int64_t offset, int64_t length, bool skip_head,
               const shared_ptr<RangeRead> &read) {
	string buffer;
	string error;
	try {
		buffer.resize(NumericCast<idx_t>(length));
		for (int attempts = 0;; attempts++) {
			try {
				auto handle = skip_head ? fs.OpenFile(RecordFileInfo(path), FileFlags::FILE_FLAGS_READ)
				                        : fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
				handle->Read(reinterpret_cast<void *>(&buffer[0]), NumericCast<idx_t>(length),
				             NumericCast<idx_t>(offset));
				break;
			} catch (std::exception &ex) {
				ErrorData data(ex);
				if (attempts >= 3 || !IsRetryable(data)) {
					throw;
				}
				fetch_retries++;
				std::this_thread::sleep_for(std::chrono::milliseconds(100 << (attempts + 1)));
			}
		}
	} catch (std::exception &ex) {
		error = ErrorData(ex).RawMessage();
		if (error.empty()) {
			error = "failed to read the record";
		}
	} catch (...) {
		error = "unknown error";
	}
	{
		std::lock_guard<std::mutex> guard(read->lock);
		read->running--;
		if (!read->done && (error.empty() || read->running == 0)) {
			// An error only counts when no other request can still succeed
			read->done = true;
			read->data = std::move(buffer);
			read->error = std::move(error);
		}
	}
	read->finished.notify_all();
}

struct FetchedRecord {
	bool fetched = false;
	int32_t status = 0;
	string charset;
	vector<pair<string, string>> warc_headers;
	vector<pair<string, string>> http_headers;
	string body;
	string error;
	//! warc_fetch() read only the beginning of the record because it had the first matches the query needs
	bool truncated = false;
};

//! Threads shared by every warc_record() call so that parallel DuckDB tasks don't multiply the thread count
class FetchPool {
public:
	static FetchPool &Get() {
		// Never destroyed: destroying the condition variable while the threads wait on it would hang the process exit
		static auto pool = new FetchPool();
		return *pool;
	}

	//! Threads which only do range reads, so that a fetch can wait for its read with a timeout and hedge it
	static FetchPool &Reads() {
		static auto pool = new FetchPool();
		return *pool;
	}

	//! Grows the pool up to `threads`. Stops growing if the operating system has no more threads to give.
	void EnsureThreads(idx_t threads) {
		std::lock_guard<std::mutex> guard(lock);
		while (workers.size() < threads) {
			try {
				workers.emplace_back([this]() { Run(); });
				// The threads wait for tasks until the process exits
				workers.back().detach();
			} catch (std::exception &) {
				break;
			}
		}
	}

	idx_t ThreadCount() {
		std::lock_guard<std::mutex> guard(lock);
		return workers.size();
	}

	void Submit(std::function<void()> task) {
		{
			std::lock_guard<std::mutex> guard(lock);
			tasks.push_back(std::move(task));
		}
		available.notify_one();
	}

private:
	std::mutex lock;
	std::condition_variable available;
	std::deque<std::function<void()>> tasks;
	vector<std::thread> workers;

	FetchPool() = default;

	void Run() {
		while (true) {
			std::function<void()> task;
			{
				std::unique_lock<std::mutex> guard(lock);
				available.wait(guard, [this]() { return !tasks.empty(); });
				task = std::move(tasks.front());
				tasks.pop_front();
			}
			// An exception out of a thread would terminate the process, tasks report their own errors
			try {
				task();
			} catch (...) {
			}
		}
	}
};

//! Waits until every task of one vector is done
class Latch {
public:
	explicit Latch(idx_t count) : remaining(count) {
	}

	void CountDown() {
		std::lock_guard<std::mutex> guard(lock);
		if (--remaining == 0) {
			done.notify_all();
		}
	}

	void Wait() {
		std::unique_lock<std::mutex> guard(lock);
		done.wait(guard, [this]() { return remaining == 0; });
	}

private:
	std::mutex lock;
	std::condition_variable done;
	idx_t remaining;
};

//! Inflates a DEFLATE stream into `out`, at most `limit` bytes. Returns the number of consumed input bytes.
idx_t Inflate(const_data_ptr_t data, idx_t size, int window_bits, idx_t size_hint, idx_t limit, string &out) {
	out.resize(MinValue<idx_t>(MaxValue<idx_t>(size_hint, 4096), MinValue<idx_t>(limit, 16 * 1024 * 1024)));
	duckdb_miniz::mz_stream stream;
	memset(&stream, 0, sizeof(stream));
	if (duckdb_miniz::mz_inflateInit2(&stream, window_bits) != duckdb_miniz::MZ_OK) {
		throw IOException("inflate init failed");
	}
	stream.next_in = data;
	stream.avail_in = NumericCast<unsigned int>(size);
	idx_t produced = 0;
	int status;
	do {
		if (produced == out.size()) {
			if (out.size() >= limit) {
				duckdb_miniz::mz_inflateEnd(&stream);
				throw IOException("record is larger than %llu bytes after decompression", limit);
			}
			out.resize(MinValue<idx_t>(out.size() * 2, limit));
		}
		stream.next_out = data_ptr_cast(&out[produced]);
		stream.avail_out = NumericCast<unsigned int>(out.size() - produced);
		status = duckdb_miniz::mz_inflate(&stream, duckdb_miniz::MZ_NO_FLUSH);
		produced = out.size() - stream.avail_out;
	} while (status == duckdb_miniz::MZ_OK);
	idx_t consumed = size - stream.avail_in;
	duckdb_miniz::mz_inflateEnd(&stream);
	if (status != duckdb_miniz::MZ_STREAM_END) {
		throw IOException("inflate failed (%d)", status);
	}
	out.resize(produced);
	return consumed;
}

uint32_t ReadLittleEndian32(const_data_ptr_t d) {
	return d[0] | (uint32_t(d[1]) << 8) | (uint32_t(d[2]) << 16) | (uint32_t(d[3]) << 24);
}

} // namespace

// Gzip member -> bytes with libdeflate, which also checks the CRC32 and size trailer so that a wrong byte range
// doesn't return garbage. The trailer has the size of the result, so it is decompressed into a buffer of exactly that
// size in one call.
//! libdeflate checks the CRC32 and size in the gzip trailer
string InflateGzipMember(const char *data, idx_t size, idx_t limit) {
	auto d = const_data_ptr_cast(data);
	if (size < 18 || d[0] != 0x1f || d[1] != 0x8b || d[2] != 8) {
		throw IOException("not a gzip member");
	}
	// ISIZE is the size mod 2^32, records are much smaller
	idx_t expected = ReadLittleEndian32(d + size - 4);
	if (expected > limit) {
		throw IOException("record is larger than %llu bytes after decompression", limit);
	}
	struct Decompressor {
		libdeflate_decompressor *handle = libdeflate_alloc_decompressor();
		~Decompressor() {
			libdeflate_free_decompressor(handle);
		}
	};
	thread_local Decompressor decompressor;
	if (!decompressor.handle) {
		throw IOException("failed to allocate the gzip decompressor");
	}
	string out(expected, '\0');
	size_t used = 0, produced = 0;
	auto result = libdeflate_gzip_decompress_ex(decompressor.handle, data, size, &out[0], expected, &used, &produced);
	if (result != LIBDEFLATE_SUCCESS) {
		throw IOException(result == LIBDEFLATE_INSUFFICIENT_SPACE ? "the byte range is not exactly one gzip member"
		                                                          : "invalid gzip data or checksum mismatch");
	}
	if (used != size || produced != expected) {
		throw IOException("the byte range is not exactly one gzip member");
	}
	return out;
}

void RunParallel(idx_t concurrency, vector<std::function<void()>> &tasks) {
	// An exception out of a pool thread would terminate the process, the tasks report their own errors
	auto run = [](std::function<void()> &task) {
		try {
			task();
		} catch (...) {
		}
	};
	auto &pool = FetchPool::Get();
	pool.EnsureThreads(concurrency);
	if (tasks.size() <= 1 || pool.ThreadCount() == 0) {
		for (auto &task : tasks) {
			run(task);
		}
		return;
	}
	Latch latch(tasks.size());
	idx_t submitted = 0;
	try {
		for (; submitted < tasks.size(); submitted++) {
			auto &task = tasks[submitted];
			pool.Submit([&run, &task, &latch]() {
				run(task);
				latch.CountDown();
			});
		}
	} catch (...) {
	}
	// Tasks which couldn't be queued run here, the queued ones still reference the latch so always wait for them
	for (idx_t i = submitted; i < tasks.size(); i++) {
		run(tasks[i]);
		latch.CountDown();
	}
	latch.Wait();
}

namespace {

string Trim(const string &s) {
	idx_t b = 0, e = s.size();
	while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r')) {
		b++;
	}
	while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r')) {
		e--;
	}
	return s.substr(b, e - b);
}


// "Name: value\r\n..." -> pairs with lower case names. Returns the first line separately.
string ParseHeaderBlock(const string &raw, idx_t begin, idx_t end, vector<pair<string, string>> &headers) {
	idx_t pos = begin;
	string first_line;
	bool first = true;
	while (pos < end) {
		idx_t nl = raw.find('\n', pos);
		if (nl == string::npos || nl > end) {
			nl = end;
		}
		string line = Trim(raw.substr(pos, nl - pos));
		pos = nl + 1;
		if (line.empty()) {
			continue;
		}
		if (first) {
			first_line = std::move(line);
			first = false;
			continue;
		}
		idx_t colon = line.find(':');
		if (colon == string::npos) {
			continue;
		}
		// Header values are not always UTF-8
		headers.emplace_back(MakeValidUtf8(StringUtil::Lower(Trim(line.substr(0, colon)))),
		                     MakeValidUtf8(Trim(line.substr(colon + 1))));
	}
	return first_line;
}

idx_t FindBlankLine(const string &s, idx_t from, idx_t &separator_length) {
	idx_t crlf = s.find("\r\n\r\n", from);
	idx_t lf = s.find("\n\n", from);
	if (crlf != string::npos && (lf == string::npos || crlf <= lf)) {
		separator_length = 4;
		return crlf;
	}
	separator_length = 2;
	return lf;
}

const string *FindHeader(const vector<pair<string, string>> &headers, const string &name) {
	for (auto &header : headers) {
		if (header.first == name) {
			return &header.second;
		}
	}
	return nullptr;
}

// HTTP Content-Encoding of the archived response. Common Crawl usually stores decoded bodies, but not always.
string DecodeContentEncoding(const string &encoding, const string &body) {
	auto value = StringUtil::Lower(encoding);
	if (value == "gzip" || value == "x-gzip") {
		return InflateGzipMember(body.data(), body.size(), MAX_RECORD_BYTES);
	}
	if (value == "deflate") {
		string out;
		// zlib wrapped, or raw DEFLATE from servers which get it wrong
		auto d = const_data_ptr_cast(body.data());
		bool zlib = body.size() >= 2 && (d[0] & 0x0F) == 8 && ((d[0] << 8) | d[1]) % 31 == 0;
		Inflate(d, body.size(), zlib ? MZ_DEFAULT_WINDOW_BITS : -MZ_DEFAULT_WINDOW_BITS, body.size() * 4,
		        MAX_RECORD_BYTES, out);
		return out;
	}
	if (value.empty() || value == "identity") {
		return body;
	}
	throw IOException("unsupported Content-Encoding \"%s\"", encoding);
}

void ParseWarc(const string &raw, const string &charset_hint, FetchedRecord &record) {
	idx_t separator = 0;
	idx_t warc_end = FindBlankLine(raw, 0, separator);
	if (warc_end == string::npos || raw.compare(0, 5, "WARC/") != 0) {
		throw IOException("not a WARC record");
	}
	ParseHeaderBlock(raw, 0, warc_end, record.warc_headers);
	idx_t body_start = warc_end + separator;
	auto content_type = FindHeader(record.warc_headers, "content-type");
	bool is_http = content_type && content_type->find("application/http") != string::npos;
	if (is_http && raw.compare(body_start, 5, "HTTP/") == 0) {
		idx_t http_end = FindBlankLine(raw, body_start, separator);
		if (http_end == string::npos) {
			http_end = raw.size();
			separator = 0;
		}
		string status_line = ParseHeaderBlock(raw, body_start, http_end, record.http_headers);
		auto space = status_line.find(' ');
		if (space != string::npos) {
			record.status = NumericCast<int32_t>(std::atoi(status_line.c_str() + space + 1));
		}
		body_start = MinValue<idx_t>(http_end + separator, raw.size());
	}
	idx_t body_end = raw.size();
	// The gzip member ends with the record separator
	if (body_end >= body_start + 4 && raw.compare(body_end - 4, 4, "\r\n\r\n") == 0) {
		body_end -= 4;
	}
	record.body = raw.substr(body_start, body_end - body_start);

	auto encoding = FindHeader(record.http_headers, "content-encoding");
	if (encoding) {
		record.body = DecodeContentEncoding(*encoding, record.body);
	}

	record.body = ToUtf8(record.body, FindHeader(record.http_headers, "content-type"), charset_hint, record.charset);
}

} // namespace

bool IsRetryable(const ErrorData &error) {
	auto &info = error.ExtraInfo();
	auto status = info.find("status_code");
	if (status != info.end()) {
		auto code = std::atoi(status->second.c_str());
		return code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
	}
	// No HTTP response at all: connection problems and timeouts
	return error.Type() == ExceptionType::IO || error.Type() == ExceptionType::HTTP;
}

namespace {

// The record is a byte range of a much larger file. Telling httpfs the file metadata up front skips the HEAD request
// it would otherwise send before every read. Range reads don't use the file size and the empty ETag makes the GET
// unconditional. httpfs keeps the first metadata of a path for the rest of the query, so the same values are used for
// every record: a size larger than any WARC file so that a full download threshold can never trigger.
OpenFileInfo RecordFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	auto &options = info.extended_info->options;
	options["file_size"] = Value::UBIGINT(uint64_t(1) << 40);
	options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	options["etag"] = Value("");
	return info;
}

Value HeadersToMap(const vector<pair<string, string>> &headers) {
	// MAP keys must be unique so repeated headers are joined like HTTP does
	vector<string> order;
	unordered_map<string, string> merged;
	for (auto &header : headers) {
		auto entry = merged.find(header.first);
		if (entry == merged.end()) {
			order.push_back(header.first);
			merged[header.first] = header.second;
		} else {
			entry->second += ", " + header.second;
		}
	}
	vector<Value> keys, values;
	for (auto &key : order) {
		keys.emplace_back(key);
		values.emplace_back(merged[key]);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

struct FetchJob {
	idx_t row;
	string path;
	int64_t offset;
	int64_t length;
	string charset_hint;
};

//! Lambda allows 1024 open files, every request in flight holds a connection
constexpr idx_t MAX_ASYNC_REQUESTS = 768;
//! Threads for reading local files, remote ones are read by the async fetcher
constexpr idx_t LOCAL_READ_THREADS = 32;
//! Threads which parse the beginnings of records
constexpr idx_t STREAM_THREADS = 8;
//! Compressed bytes fetched first when the query only needs first matches, ~200KB of HTML
constexpr int64_t STREAM_FIRST_BYTES = 32 * 1024;

struct FetchSettings {
	explicit FetchSettings(ClientContext &context)
	    : context(context.shared_from_this()), fs(FileSystem::GetFileSystem(context)),
	      base_path(GetSetting<string>(context, "warc_base_path", "s3://commoncrawl/")),
	      concurrency(MaxValue<idx_t>(1, GetSetting<idx_t>(context, "warc_concurrency", 32))),
	      skip_head(GetSetting<bool>(context, "warc_skip_head", true)),
	      hedge_ms(GetSetting<idx_t>(context, "warc_hedge_ms", 1000)) {
		concurrency = MinValue<idx_t>(concurrency, MAX_ASYNC_REQUESTS);
		SetAsyncConcurrency(concurrency);
	}

	//! Credentials of the S3 secret for a bucket, looked up once
	optional_ptr<const S3Credentials> Credentials(const string &path) {
		auto bucket = path.substr(0, path.find('/', 5));
		auto entry = credentials.find(bucket);
		if (entry == credentials.end()) {
			S3Credentials found;
			bool has = S3Credentials::Lookup(*context, path, found);
			entry = credentials.emplace(bucket, std::make_pair(has, found)).first;
		}
		return entry->second.first ? &entry->second.second : nullptr;
	}

	//! Keeps the file system alive for reads which outlive the query
	shared_ptr<ClientContext> context;
	unordered_map<string, std::pair<bool, S3Credentials>> credentials;
	FileSystem &fs;
	string base_path;
	idx_t concurrency;
	bool skip_head;
	idx_t hedge_ms;
};

//! The fetch of row `index` of the argument vectors, false if an argument is NULL
bool MakeJob(const FetchSettings &settings, UnifiedVectorFormat &files, UnifiedVectorFormat &offsets,
             UnifiedVectorFormat &lengths, optional_ptr<UnifiedVectorFormat> hints, idx_t index, idx_t row,
             FetchJob &job) {
	auto fi = files.sel->get_index(index), oi = offsets.sel->get_index(index), li = lengths.sel->get_index(index);
	if (!files.validity.RowIsValid(fi) || !offsets.validity.RowIsValid(oi) || !lengths.validity.RowIsValid(li)) {
		return false;
	}
	auto file = UnifiedVectorFormat::GetData<string_t>(files)[fi].GetString();
	bool absolute = file.find("://") != string::npos || (!file.empty() && file[0] == '/');
	job.row = row;
	job.path = absolute ? file : settings.base_path + file;
	job.offset = UnifiedVectorFormat::GetData<int64_t>(offsets)[oi];
	job.length = UnifiedVectorFormat::GetData<int64_t>(lengths)[li];
	job.charset_hint.clear();
	if (hints) {
		auto hi = hints->sel->get_index(index);
		if (hints->validity.RowIsValid(hi)) {
			job.charset_hint = UnifiedVectorFormat::GetData<string_t>(*hints)[hi].GetString();
		}
	}
	return true;
}

//! First match selectors of the query which reads the records, see SetWarcFetchStreamSelectors()
using StreamSelectors = shared_ptr<const vector<string>>;

//! Inflates the beginning of a gzip member and parses its HTML body incrementally until every selector the query
//! needs has its final first match
class StreamingRecord {
public:
	explicit StreamingRecord(StreamSelectors selectors_p) : selectors(std::move(selectors_p)) {
		memset(&stream, 0, sizeof(stream));
		initialized = inflateInit2(&stream, 16 + MAX_WBITS) == Z_OK;
		failed = !initialized;
	}
	~StreamingRecord() {
		if (initialized) {
			inflateEnd(&stream);
		}
	}

	//! false once the record has what the query needs, or when the data isn't a gzip member
	bool Feed(const char *data, idx_t size) {
		if (failed || finished) {
			return !failed;
		}
		stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(data));
		stream.avail_in = uInt(size);
		char buffer[65536];
		while (stream.avail_in > 0) {
			stream.next_out = reinterpret_cast<Bytef *>(buffer);
			stream.avail_out = sizeof(buffer);
			auto status = inflate(&stream, Z_NO_FLUSH);
			out.append(buffer, sizeof(buffer) - stream.avail_out);
			if (status == Z_STREAM_END) {
				finished = true;
				break;
			}
			if ((status != Z_OK && status != Z_BUF_ERROR) || out.size() > MAX_RECORD_BYTES) {
				failed = true;
				return false;
			}
			if (stream.avail_out == sizeof(buffer)) {
				break;
			}
		}
		try {
			return finished || !Found();
		} catch (std::exception &) {
			// The page can't be parsed incrementally, read all of it
			matcher.reset();
			gave_up = true;
			return true;
		}
	}

	string out;
	bool failed = false;
	bool finished = false;

private:
	z_stream stream;
	bool initialized = false;
	StreamSelectors selectors;
	unique_ptr<HtmlStreamMatcher> matcher;
	idx_t body_start = string::npos;
	idx_t parsed = 0;
	//! A body with Content-Encoding or selectors which can't be streamed: the record is read completely
	bool gave_up = false;

	bool FindBody() {
		auto warc_end = out.find("\r\n\r\n");
		if (warc_end == string::npos) {
			return false;
		}
		auto position = warc_end + 4;
		if (out.compare(position, 5, "HTTP/") == 0) {
			auto http_end = out.find("\r\n\r\n", position);
			if (http_end == string::npos) {
				return false;
			}
			auto headers = StringUtil::Lower(out.substr(position, http_end - position));
			auto encoding = headers.find("\ncontent-encoding:");
			if (encoding != string::npos) {
				auto line_end = headers.find('\r', encoding + 1);
				auto value = Trim(headers.substr(encoding + 18, line_end == string::npos ? string::npos
				                                                                           : line_end - encoding - 18));
				gave_up = value != "identity";
			}
			position = http_end + 4;
		}
		body_start = position;
		parsed = position;
		if (!gave_up) {
			matcher = HtmlStreamMatcher::Create(*selectors);
			gave_up = !matcher;
		}
		return true;
	}

	bool Found() {
		if (body_start == string::npos && !FindBody()) {
			return false;
		}
		if (gave_up || parsed >= out.size()) {
			return false;
		}
		auto found = matcher->Feed(out.data() + parsed, out.size() - parsed);
		parsed = out.size();
		return found;
	}
};

//! The result of one fetch, filled by the async fetcher
struct FetchSlot {
	bool done = false;
	//! data is the inflated record instead of the gzip member
	bool decompressed = false;
	bool truncated = false;
	string data;
	string error;
};

struct FetchWaiter {
	std::mutex lock;
	std::condition_variable finished;
};

//! Waits until done() is true, checking every 100ms whether the query was interrupted, for example by the lambda's
//! deadline. The fetches keep their own references to the slots, so they can finish after this threw.
template <class F>
void WaitForFetches(ClientContext &context, FetchWaiter &waiter, F done) {
	std::unique_lock<std::mutex> guard(waiter.lock);
	while (!waiter.finished.wait_for(guard, std::chrono::milliseconds(100), done)) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	}
}

bool IsRemote(const string &path) {
	return StringUtil::StartsWith(path, "s3://") || StringUtil::StartsWith(path, "https://") ||
	       StringUtil::StartsWith(path, "http://");
}

//! Starts fetching a record. The slot is filled and the waiter notified when it's done, never throws.
//! When the query only needs first matches of selectors, the first STREAM_FIRST_BYTES of the record are fetched and
//! parsed first and the rest only if the matches weren't in them. Aborting a transfer as soon as the matches arrive
//! would close its connection, and new TLS connections cost more than the bytes saved.
void StartFetch(FetchSettings &settings, const FetchJob &job, const StreamSelectors &stream_selectors,
                const shared_ptr<FetchWaiter> &waiter, const shared_ptr<FetchSlot> &slot) {
	auto finish = [waiter, slot](string data, string error, bool decompressed, bool truncated) {
		{
			std::lock_guard<std::mutex> guard(waiter->lock);
			slot->data = std::move(data);
			slot->error = std::move(error);
			slot->decompressed = decompressed;
			slot->truncated = truncated;
			slot->done = true;
		}
		waiter->finished.notify_all();
	};
	try {
		if (job.offset < 0 || job.length <= 0 || job.offset > NumericLimits<int64_t>::Maximum() - job.length) {
			finish(string(), "invalid offset or length", false, false);
			return;
		}
		if (NumericCast<idx_t>(job.length) > MAX_COMPRESSED_BYTES) {
			finish(string(), StringUtil::Format("record is larger than %llu bytes", MAX_COMPRESSED_BYTES), false,
			       false);
			return;
		}
		if (!IsRemote(job.path)) {
			// Local files are read with DuckDB's file system on the fetch pool
			auto &pool = FetchPool::Get();
			pool.EnsureThreads(LOCAL_READ_THREADS);
			auto context = settings.context;
			auto &fs = settings.fs;
			auto skip_head = settings.skip_head;
			pool.Submit([context, &fs, job, skip_head, finish]() {
				auto read = make_shared_ptr<RangeRead>();
				read->running = 1;
				ReadRange(fs, job.path, job.offset, job.length, skip_head, read);
				finish(std::move(read->data), std::move(read->error), false, false);
			});
			return;
		}
		// The async fetcher doesn't go through DuckDB's file system, so check allowed_directories here
		if (!DBConfig::GetConfig(*settings.context).CanAccessFile(job.path, FileType::FILE_TYPE_REGULAR)) {
			finish(string(), "Permission Error: cannot access " + job.path, false, false);
			return;
		}
		optional_ptr<const S3Credentials> credentials;
		if (StringUtil::StartsWith(job.path, "s3://")) {
			credentials = settings.Credentials(job.path);
		}
		auto hedge_ms = settings.hedge_ms;
		if (stream_selectors && job.length > STREAM_FIRST_BYTES) {
			FetchPool::Get().EnsureThreads(STREAM_THREADS);
			auto first = MakeRangeRequest(job.path, job.offset, STREAM_FIRST_BYTES, credentials);
			first.hedge_ms = hedge_ms;
			// The credentials of the bucket are cached by the settings which live as long as the query
			auto rest = make_shared_ptr<AsyncRequest>(MakeRangeRequest(
			    job.path, job.offset + STREAM_FIRST_BYTES, job.length - STREAM_FIRST_BYTES, credentials));
			rest->hedge_ms = hedge_ms;
			first.on_done = [stream_selectors, rest, finish](string head, string error) {
				if (!error.empty()) {
					finish(string(), std::move(error), false, false);
					return;
				}
				// Parsing doesn't belong on the fetcher thread
				auto data = make_shared_ptr<string>(std::move(head));
				FetchPool::Get().Submit([stream_selectors, rest, finish, data]() {
					try {
						StreamingRecord record(stream_selectors);
						auto more = record.Feed(data->data(), data->size());
						if (!more && !record.failed) {
							// The CRC32 is in the trailer at the end of the record, it can't be checked without the rest
							finish(std::move(record.out), string(), true, true);
							return;
						}
						rest->on_done = [finish, data](string tail, string error) {
							if (!error.empty()) {
								finish(string(), std::move(error), false, false);
								return;
							}
							// Inflated with the CRC32 check like a record fetched at once
							data->append(tail);
							finish(std::move(*data), string(), false, false);
						};
						SubmitAsyncRequest(std::move(*rest));
					} catch (std::exception &ex) {
						finish(string(), ErrorData(ex).RawMessage(), false, false);
					}
				});
			};
			SubmitAsyncRequest(std::move(first));
			return;
		}
		auto request = MakeRangeRequest(job.path, job.offset, job.length, credentials);
		request.hedge_ms = hedge_ms;
		request.on_done = [finish](string data, string error) {
			finish(std::move(data), std::move(error), false, false);
		};
		SubmitAsyncRequest(std::move(request));
	} catch (std::exception &ex) {
		finish(string(), ErrorData(ex).RawMessage(), false, false);
	}
}

//! Inflates and parses a fetched record, errors are in the record
void DecodeSlot(const FetchJob &job, FetchSlot &slot, FetchedRecord &record) {
	if (!slot.error.empty()) {
		record.error = std::move(slot.error);
		return;
	}
	try {
		auto raw = slot.decompressed ? std::move(slot.data)
		                             : InflateGzipMember(slot.data.data(), slot.data.size(), MAX_RECORD_BYTES);
		string().swap(slot.data);
		ParseWarc(raw, job.charset_hint, record);
		record.truncated = slot.truncated;
		record.fetched = true;
	} catch (std::exception &ex) {
		record.fetched = false;
		record.error = ErrorData(ex).RawMessage();
	}
}

//! Writes a record into row i of a warc_record() struct vector, NULL when there is no record
void WriteRecord(Vector &result, idx_t i, FetchedRecord *record) {
	auto &children = StructVector::GetEntries(result);
	if (!record) {
		// Children of a NULL struct must be NULL too
		FlatVector::SetNull(result, i, true);
		for (auto &child : children) {
			FlatVector::SetNull(child, i, true);
		}
		return;
	}
	if (record->fetched) {
		FlatVector::GetDataMutable<int32_t>(children[0])[i] = record->status;
		children[1].SetValue(i, HeadersToMap(record->http_headers));
		FlatVector::GetDataMutable<string_t>(children[2])[i] = StringVector::AddString(children[2], record->body);
		// The body is in the vector now, free the copy so that the vector doesn't need twice the memory
		string().swap(record->body);
		FlatVector::GetDataMutable<string_t>(children[3])[i] = StringVector::AddString(children[3], record->charset);
		children[4].SetValue(i, HeadersToMap(record->warc_headers));
		FlatVector::SetNull(children[5], i, true);
		FlatVector::GetDataMutable<bool>(children[6])[i] = record->truncated;
	} else {
		for (idx_t c = 0; c < 5; c++) {
			FlatVector::SetNull(children[c], i, true);
		}
		FlatVector::SetNull(children[6], i, true);
		FlatVector::GetDataMutable<string_t>(children[5])[i] =
		    StringVector::AddString(children[5], MakeValidUtf8(record->error));
	}
}

//! Fetches the records of rows [0, count) of the argument vectors into a warc_record() struct vector
void FetchIntoStruct(ClientContext &context, UnifiedVectorFormat &files, UnifiedVectorFormat &offsets,
                     UnifiedVectorFormat &lengths, optional_ptr<UnifiedVectorFormat> hints, idx_t count,
                     Vector &result) {
	FetchSettings settings(context);
	vector<FetchJob> jobs;
	vector<bool> valid(count, false);
	idx_t compressed_bytes = 0;
	for (idx_t i = 0; i < count; i++) {
		FetchJob job;
		if (MakeJob(settings, files, offsets, lengths, hints, i, i, job)) {
			valid[i] = true;
			compressed_bytes += NumericCast<idx_t>(MaxValue<int64_t>(job.length, 0));
			jobs.push_back(std::move(job));
		}
	}
	// Fetched records are about 8 times their compressed size until they are in the result vector
	FetchBudget budget(compressed_bytes * 8);
	auto waiter = make_shared_ptr<FetchWaiter>();
	vector<shared_ptr<FetchSlot>> slots(count);
	for (auto &job : jobs) {
		slots[job.row] = make_shared_ptr<FetchSlot>();
		StartFetch(settings, job, nullptr, waiter, slots[job.row]);
	}
	WaitForFetches(*settings.context, *waiter, [&]() {
		for (auto &job : jobs) {
			if (!slots[job.row]->done) {
				return false;
			}
		}
		return true;
	});
	result.SetVectorType(VectorType::FLAT_VECTOR);
	idx_t next_job = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!valid[i]) {
			WriteRecord(result, i, nullptr);
			continue;
		}
		FetchedRecord record;
		DecodeSlot(jobs[next_job++], *slots[i], record);
		WriteRecord(result, i, &record);
	}
}

void WarcRecordFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// warc_record() is volatile so DuckDB doesn't reduce constant arguments to one row, fetch them only once
	bool constant = args.AllConstant();
	idx_t count = constant ? 1 : args.size();
	UnifiedVectorFormat files, offsets, lengths, hints;
	args.data[0].ToUnifiedFormat(files);
	args.data[1].ToUnifiedFormat(offsets);
	args.data[2].ToUnifiedFormat(lengths);
	bool has_hint = args.ColumnCount() > 3;
	if (has_hint) {
		args.data[3].ToUnifiedFormat(hints);
	}
	FetchIntoStruct(state.GetContext(), files, offsets, lengths, has_hint ? &hints : nullptr, count, result);
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

// warc_fetch(TABLE) -> the input columns and `record`, the same struct as warc_record() returns
//
//   FROM warc_fetch((SELECT url, warc_filename, warc_record_offset, warc_record_length FROM archives WHERE ...))
//
// warc_record() has to return a whole vector of 2048 records at once, which is gigabytes for large pages, and each
// vector waits for its slowest record. warc_fetch() keeps fetching up to WARC_FETCH_WINDOW_BYTES of compressed records
// ahead and emits the finished rows in order in chunks of about WARC_FETCH_CHUNK_BYTES, so memory stays bounded and a
// slow record holds back only its own chunk while the fetching continues. content_charset is used as the charset hint
// when the input has it.

constexpr idx_t WARC_FETCH_CHUNK_BYTES = 4 * 1024 * 1024;
constexpr idx_t WARC_FETCH_WINDOW_BYTES = 32 * 1024 * 1024;

struct WarcFetchBindData : public TableFunctionData {
	idx_t file_column = 0, offset_column = 0, length_column = 0;
	optional_idx charset_column;
	//! Set by the optimizer when the query only needs the first matches of these selectors from the body
	StreamSelectors stream_selectors;
};

//! Records of one input chunk which the fetch pool fills while the in-out function waits for them in order
struct WarcFetchWindow : public FetchWaiter {
	vector<shared_ptr<FetchSlot>> slots;
};

struct WarcFetchLocalState : public LocalTableFunctionState {
	//! Shared with the fetch tasks, which can outlive this state when the query is cancelled
	shared_ptr<FetchSettings> settings;
	shared_ptr<WarcFetchWindow> window;
	vector<FetchJob> jobs;
	vector<bool> valid;
	//! Next row to submit, next row to emit and the compressed bytes submitted but not emitted
	idx_t next_submit = 0;
	idx_t next_emit = 0;
	idx_t window_bytes = 0;
	Vector offsets = Vector(LogicalType::BIGINT);
	Vector lengths = Vector(LogicalType::BIGINT);
};

LogicalType WarcRecordType() {
	auto headers = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	child_list_t<LogicalType> children = {{"status", LogicalType::INTEGER},   {"headers", headers},
	                                      {"body", LogicalType::VARCHAR},     {"charset", LogicalType::VARCHAR},
	                                      {"warc", headers},                  {"error", LogicalType::VARCHAR},
	                                      {"truncated", LogicalType::BOOLEAN}};
	return LogicalType::STRUCT(children);
}

unique_ptr<FunctionData> WarcFetchBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto data = make_uniq<WarcFetchBindData>();
	optional_idx file, offset, length;
	for (idx_t i = 0; i < input.input_table_names.size(); i++) {
		auto name = StringUtil::Lower(input.input_table_names[i].GetIdentifierName());
		if (name == "warc_filename") {
			file = i;
		} else if (name == "warc_record_offset") {
			offset = i;
		} else if (name == "warc_record_length") {
			length = i;
		} else if (name == "content_charset" && input.input_table_types[i].id() == LogicalTypeId::VARCHAR) {
			data->charset_column = i;
		}
		return_types.push_back(input.input_table_types[i]);
		names.push_back(input.input_table_names[i]);
	}
	if (!file.IsValid() || !offset.IsValid() || !length.IsValid()) {
		throw BinderException("warc_fetch needs the columns warc_filename, warc_record_offset and warc_record_length");
	}
	if (input.input_table_types[file.GetIndex()].id() != LogicalTypeId::VARCHAR) {
		throw BinderException("warc_fetch: warc_filename must be a VARCHAR");
	}
	data->file_column = file.GetIndex();
	data->offset_column = offset.GetIndex();
	data->length_column = length.GetIndex();
	return_types.push_back(WarcRecordType());
	names.emplace_back("record");
	return std::move(data);
}

unique_ptr<LocalTableFunctionState> WarcFetchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	auto state = make_uniq<WarcFetchLocalState>();
	state->settings = make_shared_ptr<FetchSettings>(context.client);
	return std::move(state);
}

//! Submits rows to the fetch pool until the window is full
void FillWindow(WarcFetchLocalState &state, const StreamSelectors &stream_selectors) {
	while (state.next_submit < state.jobs.size() &&
	       (state.window_bytes < WARC_FETCH_WINDOW_BYTES || state.next_submit == state.next_emit)) {
		auto row = state.next_submit++;
		auto &slot = state.window->slots[row];
		slot = make_shared_ptr<FetchSlot>();
		if (!state.valid[row]) {
			std::lock_guard<std::mutex> guard(state.window->lock);
			slot->done = true;
			continue;
		}
		state.window_bytes += NumericCast<idx_t>(MaxValue<int64_t>(state.jobs[row].length, 0));
		StartFetch(*state.settings, state.jobs[row], stream_selectors, state.window, slot);
	}
}

OperatorResultType WarcFetchFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                     DataChunk &output) {
	auto &data = data_p.bind_data->Cast<WarcFetchBindData>();
	auto &state = data_p.local_state->Cast<WarcFetchLocalState>();
	auto count = input.size();
	if (count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	if (!state.window) {
		// A new input chunk: plan its fetches
		VectorOperations::DefaultCast(input.data[data.offset_column], state.offsets, count);
		VectorOperations::DefaultCast(input.data[data.length_column], state.lengths, count);
		UnifiedVectorFormat files, offsets, lengths, charsets;
		input.data[data.file_column].ToUnifiedFormat(files);
		state.offsets.ToUnifiedFormat(offsets);
		state.lengths.ToUnifiedFormat(lengths);
		if (data.charset_column.IsValid()) {
			input.data[data.charset_column.GetIndex()].ToUnifiedFormat(charsets);
		}
		state.jobs.assign(count, FetchJob());
		state.valid.assign(count, false);
		for (idx_t i = 0; i < count; i++) {
			state.valid[i] = MakeJob(*state.settings, files, offsets, lengths,
			                         data.charset_column.IsValid() ? &charsets : nullptr, i, i, state.jobs[i]);
		}
		state.window = make_shared_ptr<WarcFetchWindow>();
		state.window->slots.resize(count);
		state.next_submit = state.next_emit = state.window_bytes = 0;
	}
	FillWindow(state, data.stream_selectors);

	// The next rows up to WARC_FETCH_CHUNK_BYTES, at least one, all of them submitted
	auto start = state.next_emit;
	auto end = start;
	idx_t bytes = 0;
	while (end < state.next_submit && (end == start || bytes < WARC_FETCH_CHUNK_BYTES)) {
		if (state.valid[end]) {
			bytes += NumericCast<idx_t>(MaxValue<int64_t>(state.jobs[end].length, 0));
		}
		end++;
	}
	auto rows = end - start;
	auto &window = *state.window;
	WaitForFetches(*state.settings->context, window, [&]() {
		for (idx_t i = start; i < end; i++) {
			if (!window.slots[i]->done) {
				return false;
			}
		}
		return true;
	});
	auto &record = output.data[output.ColumnCount() - 1];
	record.SetVectorType(VectorType::FLAT_VECTOR);
	// Inflating and parsing runs on this DuckDB thread, so it's parallel over the threads of the query
	for (idx_t i = 0; i < rows; i++) {
		if (!state.valid[start + i]) {
			WriteRecord(record, i, nullptr);
		} else {
			FetchedRecord fetched;
			DecodeSlot(state.jobs[start + i], *window.slots[start + i], fetched);
			WriteRecord(record, i, &fetched);
		}
		window.slots[start + i].reset();
	}
	SelectionVector selection(rows);
	for (idx_t i = 0; i < rows; i++) {
		selection.set_index(i, start + i);
	}
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		output.data[c].Slice(input.data[c], selection, rows);
	}
	output.SetCardinality(rows);
	state.next_emit = end;
	state.window_bytes -= MinValue(bytes, state.window_bytes);
	// Keep the fetcher busy while DuckDB processes this chunk
	FillWindow(state, data.stream_selectors);

	if (state.next_emit < count) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	state.window.reset();
	return OperatorResultType::NEED_MORE_INPUT;
}
void WarcFetchStatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto async = GetAsyncRequestStats();
	child_list_t<Value> values = {{"fetches", Value::UBIGINT(fetch_count + async.requests)},
	                              {"retries", Value::UBIGINT(fetch_retries + async.retries)},
	                              {"errors", Value::UBIGINT(fetch_errors + async.errors)},
	                              {"slow_fetches", Value::UBIGINT(slow_fetches + async.slow)},
	                              {"max_fetch_ms", Value::UBIGINT(MaxValue(max_fetch_ms.exchange(0), async.max_ms))},
	                              {"hedges", Value::UBIGINT(fetch_hedges + async.hedges)},
	                              {"last_retry", async.last_retry.empty() ? Value(LogicalType::VARCHAR) : Value(async.last_retry)},
	                              {"slowest", async.slowest.empty() ? Value(LogicalType::VARCHAR) : Value(async.slowest)}};
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value::STRUCT(std::move(values)));
}

} // namespace

// Replaces invalid UTF-8 sequences with U+FFFD so that the value is a valid VARCHAR
string MakeValidUtf8(string s) {
	string out;
	bool copied = false;
	idx_t i = 0, n = s.size();
	while (i < n) {
		// Most of a page is ASCII, check it 8 bytes at a time
		if (i + 8 <= n) {
			uint64_t word;
			memcpy(&word, s.data() + i, 8);
			if ((word & 0x8080808080808080ULL) == 0) {
				if (copied) {
					out.append(s, i, 8);
				}
				i += 8;
				continue;
			}
		}
		unsigned char c = s[i];
		idx_t len = c < 0x80 ? 1 : (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : (c >> 3) == 30 ? 4 : 0;
		bool ok = len > 0 && i + len <= n;
		for (idx_t k = 1; ok && k < len; k++) {
			ok = ((unsigned char)s[i + k] >> 6) == 2;
		}
		if (ok && len == 2) {
			ok = c >= 0xC2;
		} else if (ok && len == 3) {
			uint32_t cp = ((c & 0x0F) << 12) | ((s[i + 1] & 0x3F) << 6) | (s[i + 2] & 0x3F);
			ok = cp >= 0x800 && (cp < 0xD800 || cp > 0xDFFF);
		} else if (ok && len == 4) {
			uint32_t cp =
			    ((c & 0x07) << 18) | ((s[i + 1] & 0x3F) << 12) | ((s[i + 2] & 0x3F) << 6) | (s[i + 3] & 0x3F);
			ok = cp >= 0x10000 && cp <= 0x10FFFF;
		}
		if (!ok) {
			if (!copied) {
				out.assign(s, 0, i);
				copied = true;
			}
			out += "\xEF\xBF\xBD";
			i++;
			continue;
		}
		if (copied) {
			out.append(s, i, len);
		}
		i += len;
	}
	return copied ? out : s;
}

void SetWarcFetchStreamSelectors(FunctionData &bind_data, vector<string> selectors) {
	bind_data.Cast<WarcFetchBindData>().stream_selectors = make_shared_ptr<const vector<string>>(std::move(selectors));
}

bool IsWarcFetch(const string &function_name) {
	return function_name == "warc_fetch";
}

void RegisterWarcFunctions(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("warc_base_path", "Prefix for relative WARC file names", LogicalType::VARCHAR,
	                          Value("s3://commoncrawl/"));
	config.AddExtensionOption("warc_concurrency", "Threads which fetch warc_record() records, shared by all queries",
	                          LogicalType::UBIGINT, Value::UBIGINT(32));
	config.AddExtensionOption("warc_hedge_ms",
	                          "Start a second request for a WARC record which hasn't arrived in this many milliseconds, 0 "
	                          "disables it",
	                          LogicalType::UBIGINT, Value::UBIGINT(1000));
	config.AddExtensionOption("warc_skip_head", "Read WARC records without a HEAD request first",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	auto headers = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	child_list_t<LogicalType> children = {{"status", LogicalType::INTEGER},   {"headers", headers},
	                                      {"body", LogicalType::VARCHAR},     {"charset", LogicalType::VARCHAR},
	                                      {"warc", headers},                  {"error", LogicalType::VARCHAR},
	                                      {"truncated", LogicalType::BOOLEAN}};
	ScalarFunctionSet warc_record("warc_record");
	// The optional fourth argument is the charset to use when the page doesn't declare one, e.g. content_charset
	for (auto arguments : {vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                       vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
	                                            LogicalType::VARCHAR}}) {
		ScalarFunction function("warc_record", arguments, LogicalType::STRUCT(children), WarcRecordFunction);
		// Network reads: don't let the optimizer duplicate or constant fold calls
		function.SetVolatile();
		// Problems with single records are in the error field, building the result can still run out of memory
		function.SetFallible();
		warc_record.AddFunction(function);
	}
	loader.RegisterFunction(warc_record);

	// Counters since the process started, max_fetch_ms is reset by every call
	child_list_t<LogicalType> stats = {{"fetches", LogicalType::UBIGINT},
	                                   {"retries", LogicalType::UBIGINT},
	                                   {"errors", LogicalType::UBIGINT},
	                                   {"slow_fetches", LogicalType::UBIGINT},
	                                   {"max_fetch_ms", LogicalType::UBIGINT},
	                                   {"hedges", LogicalType::UBIGINT},
	                                   {"last_retry", LogicalType::VARCHAR},
	                                   {"slowest", LogicalType::VARCHAR}};
	TableFunction warc_fetch("warc_fetch", FunctionSignature().AddPositionalOnly("input", LogicalType::TABLE), nullptr,
	                         WarcFetchBind, nullptr, WarcFetchInitLocal);
	warc_fetch.in_out_function = WarcFetchFunction;

	loader.RegisterFunction(warc_fetch);

	ScalarFunction warc_fetch_stats("warc_fetch_stats", {}, LogicalType::STRUCT(stats), WarcFetchStatsFunction);
	warc_fetch_stats.SetVolatile();
	loader.RegisterFunction(warc_fetch_stats);
}

} // namespace duckdb

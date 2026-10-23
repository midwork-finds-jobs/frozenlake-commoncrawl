#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "duckdb/common/error_data.hpp"

#include <functional>

namespace duckdb {

//! warc_record(filename, offset, length[, charset]): fetches and parses one WARC record
void RegisterWarcFunctions(ExtensionLoader &loader);

//! memory_target_mb setting which moves the buffer pool limit with the memory used outside of DuckDB
void RegisterMemoryGovernor(ExtensionLoader &loader);

//! Waits while the process is close to memory_target_mb and then reserves `needed` bytes, the estimated size of the
//! next allocation, until it is destroyed. Reserving keeps parallel fetches from all passing the check at once.
class MemoryReservation {
public:
	explicit MemoryReservation(idx_t needed);
	~MemoryReservation();
	MemoryReservation(const MemoryReservation &) = delete;
	MemoryReservation &operator=(const MemoryReservation &) = delete;

private:
	idx_t bytes = 0;
};

//! Bytes which the warc_record() calls of all threads may fetch at the same time, about half of memory_target_mb.
//! One vector of large pages takes hundreds of megabytes, so a call waits while earlier calls hold the budget. A call
//! is always let through when nothing else holds it, so this can't deadlock.
class FetchBudget {
public:
	explicit FetchBudget(idx_t bytes);
	~FetchBudget();
	FetchBudget(const FetchBudget &) = delete;
	FetchBudget &operator=(const FetchBudget &) = delete;

private:
	idx_t bytes = 0;
};

//! Throttling, server errors and connection problems are retried, other HTTP errors are permanent
bool IsRetryable(const ErrorData &error);

//! Inflates one gzip member which must be exactly `size` bytes, at most `limit` bytes after decompression
string InflateGzipMember(const char *data, idx_t size, idx_t limit);

//! A range request done by the async fetcher, see async_fetch.cpp
struct AsyncRequest {
	string url;
	vector<string> headers;
	int64_t offset = 0;
	int64_t length = 0;
	//! Start a second identical request when this one hasn't finished in this many milliseconds, 0 doesn't hedge
	idx_t hedge_ms = 0;
	//! Called once on the fetcher thread with the response or an error. Must be quick and must not throw.
	std::function<void(string data, string error)> on_done;
};

struct AsyncRequestStats {
	idx_t requests = 0, retries = 0, errors = 0, slow = 0, max_ms = 0, hedges = 0;
	//! Why the last retried request failed
	string last_retry;
	//! Phases of the slowest transfer since the last call
	string slowest;
};

struct S3Credentials {
	string key_id, secret, session_token, region, endpoint;
	//! The S3 secret DuckDB would use for the path
	static bool Lookup(ClientContext &context, const string &path, S3Credentials &credentials);
};

//! A signed range request of s3:// paths, or a plain one of http(s):// urls
AsyncRequest MakeRangeRequest(const string &path, int64_t offset, int64_t length,
                              optional_ptr<const S3Credentials> credentials);
void SubmitAsyncRequest(AsyncRequest request);
void SetAsyncConcurrency(idx_t concurrency);
AsyncRequestStats GetAsyncRequestStats();

//! Runs the tasks on the process wide fetch pool with at least `concurrency` threads and waits for them.
//! The tasks must not throw.
void RunParallel(idx_t concurrency, vector<std::function<void()>> &tasks);

template <class T>
T GetSetting(ClientContext &context, const char *name, T default_value) {
	Value setting;
	if (context.TryGetCurrentSetting(name, setting) && !setting.IsNull()) {
		return setting.GetValue<T>();
	}
	return default_value;
}

//! cdx_lookup(crawl, url_surtkey, prefix): captures of the Common Crawl CDX index
void RegisterCdxFunctions(ExtensionLoader &loader);

//! Replaces invalid UTF-8 sequences with U+FFFD
string MakeValidUtf8(string s);

//! Converts a page body to UTF-8. The encoding is chosen like browsers do: byte order mark, Content-Type header,
//! <meta charset>, then the hint (for example content_charset of the index) and UTF-8 if there is nothing else.
//! `charset` is set to the WHATWG name of the encoding that was used.
string ToUtf8(const string &body, const string *content_type, const string &hint, string &charset);

//! Parses HTML as it arrives and tells when the first match of every selector is final, so that the rest of the page
//! doesn't need to be downloaded. Selectors are like in html_select_first(), including ::attr(x) and ::text.
class HtmlStreamMatcher {
public:
	//! nullptr when a selector depends on what comes after its match, like :has() or :last-child
	static unique_ptr<HtmlStreamMatcher> Create(const vector<string> &selectors);
	~HtmlStreamMatcher();
	//! Parses the next part of the body, true once every selector has its final first match
	bool Feed(const char *data, idx_t size);

private:
	HtmlStreamMatcher();
	struct State;
	unique_ptr<State> state;
};

//! Lets warc_fetch() stop reading a record once these selectors have their first match, set by the optimizer
void SetWarcFetchStreamSelectors(FunctionData &bind_data, vector<string> selectors);
//! Whether the table function is warc_fetch() and which output column is its record
bool IsWarcFetch(const string &function_name);

//! html_select, html_select_first, html_extract, html_text and js_object
void RegisterHtmlFunctions(ExtensionLoader &loader);

//! json_ld, json_ld_of_type, microdata and meta
void RegisterMacros(ExtensionLoader &loader);

//! html_json_ld_raw and html_gtins, which scan the HTML text without parsing it
void RegisterFastScanFunctions(ExtensionLoader &loader);

//! url_resolve and url_host
void RegisterUrlFunctions(ExtensionLoader &loader);

//! Combines the html_* calls on the same HTML so that it is parsed only once
void RegisterHtmlOptimizer(ExtensionLoader &loader);

class ExtractExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

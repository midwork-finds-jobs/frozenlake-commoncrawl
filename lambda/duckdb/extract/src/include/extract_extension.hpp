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

// cdx_lookup(crawl, url_surtkey, prefix) -> STRUCT(lines VARCHAR[], error VARCHAR)
//
// Captures of one url (or of every url starting with a prefix) in one crawl from Common Crawl's CDX index. The index is
// a ZipNum index: cluster.idx (about 100MB per crawl) has the first SURT key of every block of 3000 captures, the
// blocks are gzip members of cdx-NNNNN.gz. A binary search over cluster.idx with small range requests finds the
// blocks, so one lookup reads about 1MB instead of scanning the url_surtkey column of the parquet files. The old crawls
// were written without a parquet page index, so for them this is orders of magnitude faster than the archives table.
//
// Every crawl is looked up in parallel on the warc_record() fetch pool. A failed lookup of one crawl is in `error`
// so that it doesn't fail a query over every crawl. `lines` are the raw CDX lines:
//
//   fi,helsinki)/ 20240301123456 {"url": "https://www.helsinki.fi/", "mime": "text/html", "status": "200", ...}

#include "extract_extension.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/config.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

namespace {

//! Binary search stops when the remaining part of cluster.idx is this small and reads it at once
constexpr idx_t SCAN_BYTES = 256 * 1024;
constexpr idx_t PROBE_BYTES = 8 * 1024;
//! A prefix like a whole domain can match millions of captures, the archives table is better for those
constexpr idx_t MAX_BLOCKS = 64;
constexpr idx_t MAX_BLOCK_BYTES = 64 * 1024 * 1024;

struct ClusterLine {
	string key;
	string file;
	idx_t offset = 0;
	idx_t length = 0;
};

//! "fi,helsinki)/ 20240301123456\tcdx-00123.gz\t1234\t5678\t42"
bool ParseClusterLine(const string &line, ClusterLine &out) {
	auto parts = StringUtil::Split(line, '\t');
	if (parts.size() < 4) {
		return false;
	}
	out.key = parts[0].substr(0, parts[0].find(' '));
	out.file = parts[1];
	out.offset = std::stoull(parts[2]);
	out.length = std::stoull(parts[3]);
	return true;
}

string LineKey(const string &line) {
	return line.substr(0, line.find(' '));
}

template <class F>
auto WithRetries(F function) -> decltype(function()) {
	for (int attempt = 1;; attempt++) {
		try {
			return function();
		} catch (std::exception &ex) {
			if (attempt >= 4 || !IsRetryable(ErrorData(ex))) {
				throw;
			}
			// Common Crawl asks to wait at least 1s after a 503 and to back off: 1s, 2s, 4s
			std::this_thread::sleep_for(std::chrono::milliseconds(1000 << (attempt - 1)));
		}
	}
}

class CdxLookup {
public:
	CdxLookup(FileSystem &fs, string directory, string target, bool prefix)
	    : fs(fs), directory(std::move(directory)), target(std::move(target)), prefix(prefix) {
	}

	vector<string> Run() {
		vector<string> captures;
		for (auto &block : FindBlocks()) {
			auto data = ReadBlock(block);
			idx_t begin = 0;
			while (begin < data.size()) {
				auto end = data.find('\n', begin);
				if (end == string::npos) {
					end = data.size();
				}
				auto line = data.substr(begin, end - begin);
				begin = end + 1;
				auto key = LineKey(line);
				if (InRange(key)) {
					captures.push_back(MakeValidUtf8(std::move(line)));
				} else if (key > target) {
					break;
				}
			}
		}
		return captures;
	}

private:
	FileSystem &fs;
	string directory;
	string target;
	bool prefix;
	unique_ptr<FileHandle> cluster;
	idx_t cluster_size = 0;

	bool InRange(const string &key) const {
		return prefix ? StringUtil::StartsWith(key, target) : key == target;
	}

	string ReadCluster(idx_t offset, idx_t length) {
		length = MinValue<idx_t>(length, cluster_size - offset);
		string buffer(length, '\0');
		WithRetries([&]() { cluster->Read(reinterpret_cast<void *>(&buffer[0]), length, offset); });
		return buffer;
	}

	//! Blocks of cluster.idx which can contain the target: the last block starting before it and the ones starting in
	//! the range. A block starting at the target can also have captures of it at the end of the previous block.
	vector<ClusterLine> FindBlocks() {
		WithRetries([&]() {
			cluster = fs.OpenFile(directory + "cluster.idx", FileFlags::FILE_FLAGS_READ);
			cluster_size = cluster->GetFileSize();
		});
		// Invariant: the line of the first block starts in [low, high) and low is the start of a line
		idx_t low = 0, high = cluster_size;
		while (high - low > SCAN_BYTES) {
			auto middle = low + (high - low) / 2;
			// Keys of urls with long query strings can make lines longer than one probe
			string probe;
			idx_t line_begin = string::npos, line_end = string::npos;
			for (idx_t size = PROBE_BYTES; line_end == string::npos && size <= SCAN_BYTES / 2; size *= 2) {
				probe = ReadCluster(middle, size);
				line_begin = probe.find('\n');
				line_end = line_begin == string::npos ? string::npos : probe.find('\n', line_begin + 1);
			}
			if (line_end == string::npos) {
				throw IOException("unexpected line in %scluster.idx", directory);
			}
			auto key = LineKey(probe.substr(line_begin + 1, line_end - line_begin - 1));
			auto start = middle + line_begin + 1;
			if (key < target) {
				low = start;
			} else {
				high = start;
			}
		}

		vector<ClusterLine> blocks;
		idx_t position = low;
		string pending;
		while (position < cluster_size) {
			auto chunk = pending + ReadCluster(position, SCAN_BYTES);
			position += chunk.size() - pending.size();
			idx_t begin = 0;
			while (true) {
				auto end = chunk.find('\n', begin);
				if (end == string::npos) {
					if (position < cluster_size) {
						break;
					}
					end = chunk.size();
				}
				if (end > begin) {
					ClusterLine line;
					if (!ParseClusterLine(chunk.substr(begin, end - begin), line)) {
						throw IOException("unexpected line in %scluster.idx", directory);
					}
					if (line.key < target) {
						blocks = {std::move(line)};
					} else if (InRange(line.key)) {
						blocks.push_back(std::move(line));
						if (blocks.size() > MAX_BLOCKS) {
							throw InvalidInputException("cdx_lookup prefix '%s' matches more than %llu index blocks, "
							                            "use a longer prefix or the archives table",
							                            target, MAX_BLOCKS * 3000);
						}
					} else {
						return blocks;
					}
				}
				begin = end + 1;
				if (begin >= chunk.size()) {
					break;
				}
			}
			pending = begin < chunk.size() ? chunk.substr(begin) : string();
		}
		return blocks;
	}

	string ReadBlock(const ClusterLine &block) {
		if (block.length == 0 || block.length > MAX_BLOCK_BYTES || block.file.find('/') != string::npos) {
			throw IOException("unexpected block %s in %scluster.idx", block.file, directory);
		}
		MemoryReservation reservation(block.length * 8);
		string compressed(block.length, '\0');
		WithRetries([&]() {
			// Range reads of a large file, the metadata skips the HEAD request like warc_record() does
			OpenFileInfo info(directory + block.file);
			info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			info.extended_info->options["file_size"] = Value::UBIGINT(uint64_t(1) << 40);
			info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
			info.extended_info->options["etag"] = Value("");
			auto handle = fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
			handle->Read(reinterpret_cast<void *>(&compressed[0]), block.length, block.offset);
		});
		return InflateGzipMember(compressed.data(), compressed.size(), MAX_BLOCK_BYTES);
	}
};

bool ValidCrawl(const string &crawl) {
	if (crawl.empty()) {
		return false;
	}
	for (auto c : crawl) {
		if (!StringUtil::CharacterIsAlpha(c) && !StringUtil::CharacterIsDigit(c) && c != '-' && c != '_') {
			return false;
		}
	}
	return true;
}

void CdxLookupFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &fs = FileSystem::GetFileSystem(context);
	auto base_path = GetSetting<string>(context, "cdx_base_path", "s3://commoncrawl/cc-index/collections/");
	auto concurrency = MaxValue<idx_t>(1, GetSetting<idx_t>(context, "warc_concurrency", 32));

	// cdx_lookup() is volatile so DuckDB doesn't reduce constant arguments to one row, look them up only once
	bool constant = args.AllConstant();
	idx_t count = constant ? 1 : args.size();
	UnifiedVectorFormat crawls, keys, prefixes;
	args.data[0].ToUnifiedFormat(crawls);
	args.data[1].ToUnifiedFormat(keys);
	args.data[2].ToUnifiedFormat(prefixes);

	vector<bool> valid(count, false);
	vector<vector<string>> captures(count);
	vector<string> errors(count);
	vector<std::function<void()>> tasks;
	for (idx_t i = 0; i < count; i++) {
		auto ci = crawls.sel->get_index(i), ki = keys.sel->get_index(i), pi = prefixes.sel->get_index(i);
		if (!crawls.validity.RowIsValid(ci) || !keys.validity.RowIsValid(ki) || !prefixes.validity.RowIsValid(pi)) {
			continue;
		}
		valid[i] = true;
		auto crawl = UnifiedVectorFormat::GetData<string_t>(crawls)[ci].GetString();
		auto key = UnifiedVectorFormat::GetData<string_t>(keys)[ki].GetString();
		bool prefix = UnifiedVectorFormat::GetData<bool>(prefixes)[pi];
		if (!ValidCrawl(crawl)) {
			throw InvalidInputException("cdx_lookup: invalid crawl '%s'", crawl);
		}
		if (key.empty()) {
			throw InvalidInputException("cdx_lookup: empty url_surtkey");
		}
		tasks.push_back([&, i, crawl, key, prefix]() {
			// Never lets an exception out of a pool thread, that would terminate the process
			try {
				captures[i] = CdxLookup(fs, base_path + crawl + "/indexes/", key, prefix).Run();
			} catch (std::exception &ex) {
				errors[i] = crawl + ": " + ErrorData(ex).RawMessage();
			} catch (...) {
				errors[i] = crawl + ": unknown error";
			}
		});
	}
	RunParallel(concurrency, tasks);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < count; i++) {
		if (!valid[i]) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		vector<Value> lines;
		lines.reserve(captures[i].size());
		for (auto &line : captures[i]) {
			lines.emplace_back(std::move(line));
		}
		child_list_t<Value> fields = {{"lines", Value::LIST(LogicalType::VARCHAR, std::move(lines))},
		                              {"error", errors[i].empty() ? Value() : Value(MakeValidUtf8(errors[i]))}};
		result.SetValue(i, Value::STRUCT(std::move(fields)));
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

// cdx_scan(url_surtkey, prefix) -> TABLE(crawl, url_surtkey, fetch_time, cdx, error)
//
// The same lookup as a table over every crawl of the `cdx_crawls` setting. Filters on fetch_time and crawl choose the
// crawls to read, so WHERE fetch_time BETWEEN '2019-01-01' AND '2019-12-31' reads only the crawls of 2019. The filters
// are still applied to the rows afterwards. cdx_captures() is the friendlier macro over it.

struct CrawlInfo {
	string crawl;
	timestamp_t first_fetch;
	timestamp_t last_fetch;
};

//! "CC-MAIN-2026-43 2026-10-05T10:56:46 2026-10-18T05:22:30;CC-MAIN-..." set by the lambda from crawls() of the catalog
vector<CrawlInfo> ParseCrawls(const string &setting) {
	vector<CrawlInfo> crawls;
	for (auto &entry : StringUtil::Split(setting, ';')) {
		auto parts = StringUtil::Split(StringUtil::Replace(entry, "\n", ""), ' ');
		if (parts.size() != 3 || !ValidCrawl(parts[0])) {
			throw InvalidInputException("cdx_crawls has an invalid entry '%s'", entry);
		}
		crawls.push_back({parts[0], Timestamp::FromString(parts[1], false), Timestamp::FromString(parts[2], false)});
	}
	return crawls;
}

struct CdxScanData : public TableFunctionData {
	string key;
	bool prefix = false;
	vector<CrawlInfo> crawls;
};

enum CdxColumn : idx_t { CDX_CRAWL = 0, CDX_SURTKEY = 1, CDX_FETCH_TIME = 2, CDX_LINE = 3, CDX_ERROR = 4 };

unique_ptr<FunctionData> CdxScanBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto data = make_uniq<CdxScanData>();
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("cdx_scan: url_surtkey and prefix can't be NULL");
	}
	data->key = StringValue::Get(input.inputs[0]);
	data->prefix = BooleanValue::Get(input.inputs[1]);
	if (data->key.empty()) {
		throw InvalidInputException("cdx_scan: empty url_surtkey");
	}
	auto setting = GetSetting<string>(context, "cdx_crawls", "");
	if (setting.empty()) {
		throw InvalidInputException("cdx_scan needs the crawls in the cdx_crawls setting, the lambda sets it from "
		                            "crawls() of the catalog");
	}
	data->crawls = ParseCrawls(setting);
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::VARCHAR,
	                LogicalType::VARCHAR};
	names = {Identifier("crawl"), Identifier("url_surtkey"), Identifier("fetch_time"), Identifier("cdx"),
	         Identifier("error")};
	return std::move(data);
}

//! Index of the cdx_scan column if the expression is a plain reference to it
bool ScanColumn(const LogicalGet &get, const Expression &expression, idx_t &column) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	column = get.GetColumnIndex(expression.Cast<BoundColumnRefExpression>().Binding()).GetPrimaryIndex();
	return true;
}

bool ConstantValue(const Expression &expression, Value &value) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	value = expression.Cast<BoundConstantExpression>().GetValue();
	return !value.IsNull();
}

//! Narrows the crawls with filters like fetch_time BETWEEN a AND b, fetch_time >= a, crawl = x and crawl IN (...)
void NarrowCrawls(const LogicalGet &get, const Expression &filter, timestamp_t &low, timestamp_t &high,
                  unique_ptr<case_insensitive_set_t> &crawls) {
	auto type = filter.GetExpressionType();
	if (type == ExpressionType::COMPARE_IN) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		idx_t column;
		if (children.empty() || !ScanColumn(get, *children[0], column) || column != CDX_CRAWL) {
			return;
		}
		auto values = make_uniq<case_insensitive_set_t>();
		for (idx_t i = 1; i < children.size(); i++) {
			Value value;
			if (!ConstantValue(*children[i], value)) {
				return;
			}
			values->insert(value.ToString());
		}
		crawls = std::move(values);
		return;
	}
	if (filter.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return;
	}
	auto &function = filter.Cast<BoundFunctionExpression>();
	idx_t column;
	Value lower, upper;
	if (type == ExpressionType::COMPARE_BETWEEN) {
		if (ScanColumn(get, BoundBetweenExpression::Input(function), column) && column == CDX_FETCH_TIME &&
		    ConstantValue(BoundBetweenExpression::LowerBound(function), lower) &&
		    ConstantValue(BoundBetweenExpression::UpperBound(function), upper)) {
			low = MaxValue(low, lower.GetValue<timestamp_t>());
			high = MinValue(high, upper.GetValue<timestamp_t>());
		}
		return;
	}
	if (!BoundComparisonExpression::IsComparison(type) || type == ExpressionType::COMPARE_BETWEEN) {
		return;
	}
	auto *left = &BoundComparisonExpression::Left(function), *right = &BoundComparisonExpression::Right(function);
	Value value;
	if (ScanColumn(get, *right, column) && ConstantValue(*left, value)) {
		// a < column is column > a
		std::swap(left, right);
		type = FlipComparisonExpression(type);
	} else if (!ScanColumn(get, *left, column) || !ConstantValue(*right, value)) {
		return;
	}
	if (column == CDX_CRAWL && type == ExpressionType::COMPARE_EQUAL) {
		crawls = make_uniq<case_insensitive_set_t>();
		crawls->insert(value.ToString());
	} else if (column == CDX_FETCH_TIME) {
		auto timestamp = value.GetValue<timestamp_t>();
		if (type == ExpressionType::COMPARE_EQUAL) {
			low = MaxValue(low, timestamp);
			high = MinValue(high, timestamp);
		} else if (type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			low = MaxValue(low, timestamp);
		} else if (type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			high = MinValue(high, timestamp);
		}
	}
}

void CdxScanPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                     vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data->Cast<CdxScanData>();
	timestamp_t low = timestamp_t::ninfinity(), high = timestamp_t::infinity();
	unique_ptr<case_insensitive_set_t> crawls;
	for (auto &filter : filters) {
		try {
			NarrowCrawls(get, *filter, low, high, crawls);
		} catch (std::exception &) {
			// A constant which isn't a timestamp, the filter is still applied to the rows
		}
	}
	vector<CrawlInfo> selected;
	for (auto &crawl : data.crawls) {
		if (crawl.last_fetch >= low && crawl.first_fetch <= high && (!crawls || crawls->count(crawl.crawl))) {
			selected.push_back(crawl);
		}
	}
	data.crawls = std::move(selected);
}

struct CdxScanState : public GlobalTableFunctionState {
	struct Row {
		idx_t crawl;
		string line;
	};
	vector<Row> rows;
	idx_t position = 0;
};

unique_ptr<GlobalTableFunctionState> CdxScanInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<CdxScanData>();
	auto &fs = FileSystem::GetFileSystem(context);
	auto base_path = GetSetting<string>(context, "cdx_base_path", "s3://commoncrawl/cc-index/collections/");
	auto concurrency = MaxValue<idx_t>(1, GetSetting<idx_t>(context, "warc_concurrency", 32));

	vector<vector<string>> captures(data.crawls.size());
	vector<string> errors(data.crawls.size());
	vector<std::function<void()>> tasks;
	for (idx_t i = 0; i < data.crawls.size(); i++) {
		tasks.push_back([&, i]() {
			try {
				captures[i] =
				    CdxLookup(fs, base_path + data.crawls[i].crawl + "/indexes/", data.key, data.prefix).Run();
			} catch (std::exception &ex) {
				errors[i] = ErrorData(ex).RawMessage();
			} catch (...) {
				errors[i] = "unknown error";
			}
		});
	}
	RunParallel(concurrency, tasks);

	auto state = make_uniq<CdxScanState>();
	for (idx_t i = 0; i < data.crawls.size(); i++) {
		if (!errors[i].empty()) {
			// A crawl which couldn't be read is a row with only crawl and error
			state->rows.push_back({i, "\n" + MakeValidUtf8(errors[i])});
			continue;
		}
		for (auto &line : captures[i]) {
			state->rows.push_back({i, std::move(line)});
		}
	}
	return std::move(state);
}

//! "20240301123456" -> timestamp
bool ParseCdxTimestamp(const string &text, timestamp_t &result) {
	if (text.size() < 14) {
		return false;
	}
	int32_t parts[6];
	const idx_t widths[6] = {4, 2, 2, 2, 2, 2};
	idx_t position = 0;
	for (idx_t p = 0; p < 6; p++) {
		parts[p] = 0;
		for (idx_t i = 0; i < widths[p]; i++, position++) {
			if (!StringUtil::CharacterIsDigit(text[position])) {
				return false;
			}
			parts[p] = parts[p] * 10 + (text[position] - '0');
		}
	}
	date_t date;
	if (!Date::TryFromDate(parts[0], parts[1], parts[2], date) || parts[3] > 23 || parts[4] > 59 || parts[5] > 60) {
		return false;
	}
	result = Timestamp::FromDatetime(date, dtime_t(Interval::MICROS_PER_SEC * (parts[3] * 3600 + parts[4] * 60 +
	                                                                          MinValue(parts[5], 59))));
	return true;
}

void CdxScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<CdxScanData>();
	auto &state = input.global_state->Cast<CdxScanState>();
	idx_t count = 0;
	auto crawl_data = FlatVector::GetDataMutable<string_t>(output.data[CDX_CRAWL]);
	auto key_data = FlatVector::GetDataMutable<string_t>(output.data[CDX_SURTKEY]);
	auto time_data = FlatVector::GetDataMutable<timestamp_t>(output.data[CDX_FETCH_TIME]);
	auto line_data = FlatVector::GetDataMutable<string_t>(output.data[CDX_LINE]);
	auto error_data = FlatVector::GetDataMutable<string_t>(output.data[CDX_ERROR]);
	while (state.position < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.position++];
		crawl_data[count] = StringVector::AddString(output.data[CDX_CRAWL], data.crawls[row.crawl].crawl);
		if (!row.line.empty() && row.line[0] == '\n') {
			FlatVector::SetNull(output.data[CDX_SURTKEY], count, true);
			FlatVector::SetNull(output.data[CDX_FETCH_TIME], count, true);
			FlatVector::SetNull(output.data[CDX_LINE], count, true);
			error_data[count] = StringVector::AddString(output.data[CDX_ERROR], row.line.substr(1));
			count++;
			continue;
		}
		// "fi,helsinki)/ 20240301123456 {...}"
		auto key_end = row.line.find(' ');
		auto time_end = key_end == string::npos ? string::npos : row.line.find(' ', key_end + 1);
		timestamp_t timestamp;
		if (time_end == string::npos || !ParseCdxTimestamp(row.line.substr(key_end + 1, time_end - key_end - 1),
		                                                   timestamp)) {
			continue;
		}
		key_data[count] = StringVector::AddString(output.data[CDX_SURTKEY], row.line.substr(0, key_end));
		time_data[count] = timestamp;
		line_data[count] = StringVector::AddString(output.data[CDX_LINE], row.line.substr(time_end + 1));
		FlatVector::SetNull(output.data[CDX_ERROR], count, true);
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterCdxFunctions(ExtensionLoader &loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("cdx_crawls",
	                        "Crawls for cdx_scan() as 'crawl first_fetch last_fetch;...', the lambda sets it from "
	                        "crawls() of the catalog",
	                        LogicalType::VARCHAR, Value(""));
	TableFunction cdx_scan("cdx_scan", {LogicalType::VARCHAR, LogicalType::BOOLEAN}, CdxScanFunction, CdxScanBind,
	                       CdxScanInit);
	cdx_scan.pushdown_complex_filter = CdxScanPushdown;
	loader.RegisterFunction(cdx_scan);

	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("cdx_base_path", "Directory of the Common Crawl CDX index collections",
	                        LogicalType::VARCHAR, Value("s3://commoncrawl/cc-index/collections/"));
	child_list_t<LogicalType> fields = {{"lines", LogicalType::LIST(LogicalType::VARCHAR)},
	                                    {"error", LogicalType::VARCHAR}};
	ScalarFunction cdx_lookup("cdx_lookup", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                          LogicalType::STRUCT(fields), CdxLookupFunction);
	// Network reads: don't let the optimizer duplicate or constant fold calls
	cdx_lookup.SetVolatile();
	cdx_lookup.SetFallible();
	loader.RegisterFunction(cdx_lookup);
}

} // namespace duckdb

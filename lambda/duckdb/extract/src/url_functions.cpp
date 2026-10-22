// URL functions with lexbor's WHATWG URL parser, the same rules browsers use to resolve links
//
//   url_resolve(base, href) -> VARCHAR   absolute URL of a link on the page `base`, NULL if it isn't a valid URL
//   url_host(url)           -> VARCHAR   host name of an URL, NULL if it isn't a valid URL
//   surt_key(url)           -> VARCHAR   the url_surtkey Common Crawl gives the url, like fi,yle)/uutiset?a=1&b=2

#include "extract_extension.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <lexbor/url/url.h>

#include <algorithm>

namespace duckdb {

namespace {

lxb_status_t AppendToString(const lxb_char_t *data, size_t length, void *ctx) {
	static_cast<string *>(ctx)->append(const_char_ptr_cast(data), length);
	return LXB_STATUS_OK;
}

//! One lexbor URL parser, the parsed URLs live in its memory until Clean()
class UrlParser {
public:
	UrlParser() {
		if (lxb_url_parser_init(&parser, nullptr) != LXB_STATUS_OK) {
			throw InternalException("failed to initialize the lexbor URL parser");
		}
	}

	~UrlParser() {
		lxb_url_parser_memory_destroy(&parser);
		lxb_url_parser_destroy(&parser, false);
	}

	//! Parses an URL, relative to `base` if given. Previously parsed URLs stay valid until Clean().
	lxb_url_t *Parse(const lxb_url_t *base, const string_t &input) {
		lxb_url_parser_clean(&parser);
		return lxb_url_parse(&parser, base, const_data_ptr_cast(input.GetData()), input.GetSize());
	}

	//! Releases the parsed URLs, the memory would otherwise grow with every row
	void Clean() {
		lxb_url_parser_memory_destroy(&parser);
		lxb_url_parser_destroy(&parser, false);
		if (lxb_url_parser_init(&parser, nullptr) != LXB_STATUS_OK) {
			throw InternalException("failed to initialize the lexbor URL parser");
		}
	}

private:
	lxb_url_parser_t parser;
};

string Serialize(const lxb_url_t *url) {
	string out;
	lxb_url_serialize(url, AppendToString, &out, false);
	return out;
}

string SerializeHost(const lxb_url_t *url) {
	string out;
	lxb_url_serialize_host(lxb_url_host(url), AppendToString, &out);
	return out;
}

template <bool RESOLVE>
void UrlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	vector<UnifiedVectorFormat> inputs(args.ColumnCount());
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].ToUnifiedFormat(inputs[c]);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	UrlParser parser;
	for (idx_t i = 0; i < count; i++) {
		if (i % 256 == 255) {
			parser.Clean();
		}
		bool valid = true;
		vector<string_t> values;
		for (auto &input : inputs) {
			auto index = input.sel->get_index(i);
			valid = valid && input.validity.RowIsValid(index);
			values.push_back(valid ? UnifiedVectorFormat::GetData<string_t>(input)[index] : string_t());
		}
		lxb_url_t *url = nullptr;
		if (valid) {
			url = parser.Parse(nullptr, values[0]);
			if (RESOLVE && url) {
				url = parser.Parse(url, values[1]);
			}
		}
		if (!url || (!RESOLVE && lxb_url_host(url)->type == LXB_URL_HOST_TYPE__UNDEF)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		out[i] = StringVector::AddString(result, RESOLVE ? Serialize(url) : SerializeHost(url));
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

int HexValue(char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	}
	c = char(c | 0x20);
	return c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

//! Decodes %XX escapes until there are none left, like %2526 -> %26 -> &
string UnescapeRepeatedly(string text) {
	while (true) {
		string out;
		out.reserve(text.size());
		bool changed = false;
		for (idx_t i = 0; i < text.size(); i++) {
			if (text[i] == '%' && i + 2 < text.size() && HexValue(text[i + 1]) >= 0 && HexValue(text[i + 2]) >= 0) {
				out.push_back(char(HexValue(text[i + 1]) * 16 + HexValue(text[i + 2])));
				i += 2;
				changed = true;
			} else {
				out.push_back(text[i]);
			}
		}
		if (!changed) {
			return out;
		}
		text = std::move(out);
	}
}

//! Escapes control characters, spaces, non-ASCII bytes, # and % and lowercases the rest
string EscapeLower(const string &text) {
	static const char *HEX = "0123456789abcdef";
	string out;
	for (auto c : text) {
		auto byte = uint8_t(c);
		if (byte <= 0x20 || byte >= 0x7f || c == '#' || c == '%') {
			out.push_back('%');
			out.push_back(HEX[byte >> 4]);
			out.push_back(HEX[byte & 15]);
		} else {
			out.push_back(c >= 'A' && c <= 'Z' ? char(c + 32) : c);
		}
	}
	return out;
}

// The canonicalization of Common Crawl's url_surtkey, which is the Internet Archive's surt: the scheme, fragment,
// user info, default ports and www. are dropped, escapes are decoded and the bytes which need it escaped again,
// everything is lowercase, the host is reversed and the query parameters are sorted.
string SurtKey(string url) {
	// Surrounding whitespace and tabs or line breaks anywhere
	string cleaned;
	for (auto c : url) {
		if (c != '\t' && c != '\r' && c != '\n') {
			cleaned.push_back(c);
		}
	}
	auto begin = cleaned.find_first_not_of(' ');
	auto end = cleaned.find_last_not_of(' ');
	if (begin == string::npos) {
		return string();
	}
	cleaned = cleaned.substr(begin, end - begin + 1);
	auto hash = cleaned.find('#');
	if (hash != string::npos) {
		cleaned.resize(hash);
	}
	auto scheme = cleaned.find("://");
	if (scheme != string::npos && scheme > 0 && scheme < 16) {
		cleaned = cleaned.substr(scheme + 3);
	}
	auto authority_end = cleaned.find_first_of("/?");
	auto authority = cleaned.substr(0, authority_end);
	auto rest = authority_end == string::npos ? string() : cleaned.substr(authority_end);

	auto at = authority.rfind('@');
	if (at != string::npos) {
		authority = authority.substr(at + 1);
	}
	string port;
	auto colon = authority.rfind(':');
	if (colon != string::npos && authority.find(']') == string::npos) {
		port = authority.substr(colon + 1);
		authority.resize(colon);
		if (port == "80" || port == "443" || port.empty()) {
			port.clear();
		}
	}
	auto host = StringUtil::Lower(UnescapeRepeatedly(authority));
	while (!host.empty() && host.back() == '.') {
		host.pop_back();
	}
	// www, www2 and so on
	if (host.size() > 4 && host.compare(0, 3, "www") == 0) {
		idx_t i = 3;
		while (i < host.size() && StringUtil::CharacterIsDigit(host[i])) {
			i++;
		}
		if (i < host.size() && host[i] == '.') {
			host = host.substr(i + 1);
		}
	}
	auto labels = StringUtil::Split(host, '.');
	std::reverse(labels.begin(), labels.end());
	string key = StringUtil::Join(labels, ",");
	if (!port.empty()) {
		key += ":" + port;
	}
	key += ")";

	auto canonical = EscapeLower(UnescapeRepeatedly(rest));
	auto question = canonical.find('?');
	auto path = canonical.substr(0, question);
	auto query = question == string::npos ? string() : canonical.substr(question + 1);
	if (path.empty()) {
		path = "/";
	}
	if (path.size() > 1 && path.back() == '/') {
		path.pop_back();
	}
	key += path;
	if (!query.empty()) {
		auto parameters = StringUtil::Split(query, '&');
		std::sort(parameters.begin(), parameters.end());
		key += "?" + StringUtil::Join(parameters, "&");
	}
	return MakeValidUtf8(key);
}

void SurtKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t url) {
		return StringVector::AddString(result, SurtKey(url.GetString()));
	});
}

} // namespace

void RegisterUrlFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("surt_key", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SurtKeyFunction));
	loader.RegisterFunction(ScalarFunction("url_resolve", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::VARCHAR, UrlFunction<true>));
	loader.RegisterFunction(
	    ScalarFunction("url_host", {LogicalType::VARCHAR}, LogicalType::VARCHAR, UrlFunction<false>));
}

} // namespace duckdb

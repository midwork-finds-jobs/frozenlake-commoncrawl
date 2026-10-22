// Functions which scan the HTML text instead of parsing it, for data which doesn't need a DOM:
//
//   html_json_ld_raw(html) -> VARCHAR[]  contents of <script type="application/ld+json"> elements
//   html_gtins(html)       -> VARCHAR[]  GTINs next to gtin, ean, barcode or upc, valid check digit, as 14 digits
//   html_gtin_hits(html)   -> LIST(STRUCT(gtin, key, kind, offset))  every match with where and how it was found
//   html_gtin_near(html, id) -> VARCHAR  the GTIN closest to an occurrence of id, like the product id of the url
//   gtin_info(code)        -> STRUCT(gtin14, kind, prefix, flags)  GS1 prefix flags like restricted, isbn, coupon
//
// Parsing a 400KB product page with lexbor takes ~2ms and a case insensitive regex over it ~1ms, these take ~0.1ms.
// Script content is raw text in HTML5, so the JSON-LD is the same as what a parser would return.

#include "extract_extension.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

namespace {

inline char Lower(char c) {
	return c >= 'A' && c <= 'Z' ? char(c + 32) : c;
}

inline bool IsDigit(char c) {
	return c >= '0' && c <= '9';
}

inline bool IsWordCharacter(char c) {
	return IsDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

inline bool IsSpace(char c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
}

//! Whether `text` at `position` starts with the lowercase `word`, ignoring case
bool StartsWithLower(const char *text, idx_t size, idx_t position, const char *word) {
	auto length = strlen(word);
	if (position + length > size) {
		return false;
	}
	for (idx_t i = 0; i < length; i++) {
		if (Lower(text[position + i]) != word[i]) {
			return false;
		}
	}
	return true;
}

//! Finds the lowercase `word` from `position` ignoring case, size if there is none
idx_t FindLower(const char *text, idx_t size, idx_t position, const char *word) {
	auto first = word[0];
	auto upper = char(first - 32);
	while (position < size) {
		auto lower_hit = static_cast<const char *>(memchr(text + position, first, size - position));
		auto upper_hit = first >= 'a' && first <= 'z'
		                     ? static_cast<const char *>(memchr(text + position, upper, size - position))
		                     : nullptr;
		auto hit = !lower_hit ? upper_hit : !upper_hit ? lower_hit : MinValue(lower_hit, upper_hit);
		if (!hit) {
			return size;
		}
		position = idx_t(hit - text);
		if (StartsWithLower(text, size, position, word)) {
			return position;
		}
		position++;
	}
	return size;
}

//! The value of a type="..." attribute in the tag between `begin` and `end`, lowercase and trimmed
string TypeAttribute(const char *text, idx_t begin, idx_t end) {
	idx_t i = begin;
	while (i < end) {
		// Attribute name
		while (i < end && IsSpace(text[i])) {
			i++;
		}
		auto name_begin = i;
		while (i < end && !IsSpace(text[i]) && text[i] != '=' && text[i] != '>' && text[i] != '/') {
			i++;
		}
		auto name_end = i;
		if (name_end == name_begin) {
			i++;
			continue;
		}
		while (i < end && IsSpace(text[i])) {
			i++;
		}
		string value;
		if (i < end && text[i] == '=') {
			i++;
			while (i < end && IsSpace(text[i])) {
				i++;
			}
			if (i < end && (text[i] == '"' || text[i] == '\'')) {
				auto quote = text[i++];
				auto value_begin = i;
				while (i < end && text[i] != quote) {
					i++;
				}
				value.assign(text + value_begin, i - value_begin);
				i++;
			} else {
				auto value_begin = i;
				while (i < end && !IsSpace(text[i]) && text[i] != '>') {
					i++;
				}
				value.assign(text + value_begin, i - value_begin);
			}
		}
		if (name_end - name_begin == 4 && StartsWithLower(text, end, name_begin, "type")) {
			string result;
			for (auto c : value) {
				if (!IsSpace(c)) {
					result.push_back(Lower(c));
				}
			}
			return result;
		}
	}
	return string();
}

vector<string> JsonLdScripts(const char *text, idx_t size) {
	vector<string> scripts;
	idx_t position = 0;
	while ((position = FindLower(text, size, position, "<script")) < size) {
		auto tag_begin = position + 7;
		position = tag_begin;
		// "<scripts" or "<script-x" is another element
		if (tag_begin < size && !IsSpace(text[tag_begin]) && text[tag_begin] != '>' && text[tag_begin] != '/') {
			continue;
		}
		// The end of the start tag, '>' inside quoted attribute values doesn't end it
		idx_t i = tag_begin;
		char quote = 0;
		while (i < size && (quote || text[i] != '>')) {
			if (quote) {
				quote = text[i] == quote ? 0 : quote;
			} else if (text[i] == '"' || text[i] == '\'') {
				quote = text[i];
			}
			i++;
		}
		if (i >= size) {
			break;
		}
		auto content_begin = i + 1;
		auto content_end = FindLower(text, size, content_begin, "</script");
		position = content_end;
		if (TypeAttribute(text, tag_begin, i) == "application/ld+json") {
			scripts.emplace_back(text + content_begin, content_end - content_begin);
		}
	}
	return scripts;
}

bool GtinCheckDigitValid(const char *digits, idx_t length) {
	int sum = 0;
	for (idx_t i = 1; i < length; i++) {
		int digit = digits[length - 1 - i] - '0';
		sum += i % 2 == 1 ? digit * 3 : digit;
	}
	return (10 - sum % 10) % 10 == digits[length - 1] - '0';
}

struct Keyword {
	//! Lowercase spelling and the key reported for it
	const char *text;
	const char *key;
	//! Whether a word character may come right before it
	bool inside_word;
	//! Number of digits the key declares, 0 for any GTIN length
	idx_t digits;
};

// Longer spellings first so that "gtin13" wins over "gtin"
// Keys may follow a separator like data-ls-gtin or videoly-product-gtin, but not letters like notgtin
const Keyword KEYWORDS[] = {{"gtin14", "gtin14", false, 14},
                            {"gtin13", "gtin13", false, 13},
                            {"gtin12", "gtin12", false, 12},
                            {"gtin8", "gtin8", false, 8},
                            {"gtin", "gtin", false, 0},
                            {"ean-koodi", "ean", false, 0},
                            {"ean-nummer", "ean", false, 0},
                            {"ean_code", "ean", false, 0},
                            {"ean-code", "ean", false, 0},
                            {"eancode", "ean", false, 0},
                            {"eans", "ean", false, 0},
                            {"ean13", "ean13", false, 13},
                            {"ean8", "ean8", false, 8},
                            {"ean", "ean", false, 0},
                            {"barcode", "barcode", false, 0},
                            {"viivakoodi", "barcode", false, 0},
                            {"streckkod", "barcode", false, 0},
                            {"strekkode", "barcode", false, 0},
                            {"upc", "upc", false, 12},
                            {nullptr, nullptr, false, 0}};

//! The keyword at `position` and where it ends, nullptr if there is none
const Keyword *MatchKeyword(const char *text, idx_t size, idx_t position, idx_t &end) {
	for (idx_t k = 0; KEYWORDS[k].text; k++) {
		auto &keyword = KEYWORDS[k];
		if (keyword.text[0] != Lower(text[position]) || !StartsWithLower(text, size, position, keyword.text)) {
			continue;
		}
		if (!keyword.inside_word && position > 0 && IsWordCharacter(text[position - 1])) {
			return nullptr;
		}
		end = position + strlen(keyword.text);
		// "upc" and "ean" are often parts of longer words like "upcoming", the separator check rejects most of them
		return &keyword;
	}
	return nullptr;
}

struct GtinHit {
	//! 14 digits and the digits as they were on the page
	string gtin;
	string digits;
	const char *key;
	//! json: "key":"value", attr: key="value" in a tag, label: text with tags in between, text: anything else
	const char *kind;
	idx_t offset;
};

//! Skips the separator between a keyword and its value: punctuation, whitespace, escaped quotes, entities like
//! &quot; and up to 3 tags. Letters, commas and "null" end it, so that "ean":null,"updatedAt":1791523489425 or
//! "ean":"","id":12345678 don't match. Returns the position of the first digit or size.
idx_t SkipSeparator(const char *text, idx_t size, idx_t position, bool &saw_tag, bool &saw_quote, bool &saw_equals) {
	idx_t i = position, tags = 0, length = 0;
	while (i < size && length < 40) {
		auto c = text[i];
		if (IsDigit(c)) {
			return i;
		}
		if (c == '<') {
			// A tag like </dt><dd class="value">, at most 3 of them and 120 characters each
			auto close = static_cast<const char *>(memchr(text + i, '>', MinValue<idx_t>(size - i, 120)));
			if (!close || ++tags > 3) {
				return size;
			}
			saw_tag = true;
			i = idx_t(close - text) + 1;
			continue;
		}
		if (c == '&') {
			// An entity like &quot; or &#34;
			idx_t j = i + 1;
			while (j < size && j - i < 8 && (IsWordCharacter(text[j]) || text[j] == '#')) {
				j++;
			}
			if (j < size && text[j] == ';' && j > i + 1) {
				saw_quote = true;
				i = j + 1;
				length++;
				continue;
			}
			return size;
		}
		if (c == '\\') {
			// An escaped quote: \" or "
			if (i + 5 < size && text[i + 1] == 'u' && StartsWithLower(text, size, i + 2, "0022")) {
				saw_quote = true;
				i += 6;
				length++;
				continue;
			}
			i++;
			length++;
			continue;
		}
		if (c == '"' || c == '\'') {
			saw_quote = true;
		} else if (c == '=') {
			saw_equals = true;
		} else if (StartsWithLower(text, size, i, "content=")) {
			// <meta property="product:ean" content="...">
			saw_equals = true;
			i += 8;
			length += 8;
			continue;
		} else if (c != ':' && c != '[' && c != '(' && c != '{' && c != '-' && c != '#' && !IsSpace(c)) {
			// Letters, commas and anything else end the separator
			return size;
		}
		i++;
		length++;
	}
	return size;
}

//! Whether two characters, in either case, start a keyword. Checking two characters skips almost every position.
struct KeywordStarts {
	KeywordStarts() {
		for (idx_t k = 0; KEYWORDS[k].text; k++) {
			auto first = uint8_t(KEYWORDS[k].text[0]), second = uint8_t(KEYWORDS[k].text[1]);
			for (auto a : {first, uint8_t(first - 32)}) {
				for (auto b : {second, uint8_t(second - 32)}) {
					starts[a][b] = true;
				}
			}
		}
	}
	bool starts[256][256] = {{false}};
};

vector<GtinHit> GtinHits(const char *text, idx_t size) {
	static const KeywordStarts keyword_starts;
	vector<GtinHit> hits;
	for (idx_t position = 0; position + 1 < size; position++) {
		if (!keyword_starts.starts[uint8_t(text[position])][uint8_t(text[position + 1])]) {
			continue;
		}
		idx_t end;
		auto keyword = MatchKeyword(text, size, position, end);
		if (!keyword) {
			continue;
		}
		bool saw_tag = false, saw_quote = false, saw_equals = false;
		auto i = SkipSeparator(text, size, end, saw_tag, saw_quote, saw_equals);
		if (i >= size || i == end || IsWordCharacter(text[i - 1])) {
			continue;
		}
		bool array = memchr(text + end, '[', i - end) != nullptr;
		auto kind = saw_tag ? "label" : saw_equals ? "attr" : saw_quote ? "json" : "text";
		// A JSON array like "eans":["6411923686860","6411923686877"] has several values for one key
		while (true) {
			auto digits_begin = i;
			while (i < size && IsDigit(text[i])) {
				i++;
			}
			auto length = i - digits_begin;
			// Not part of a longer number like a price 12345678.90 or a range 12345678-9
			bool continues = i < size && (IsWordCharacter(text[i]) ||
			                              ((text[i] == '.' || text[i] == ',' || text[i] == '-') && i + 1 < size &&
			                               IsDigit(text[i + 1])));
			// Sites often put a UPC-12 under gtin13 or an EAN-13 under upc, but never 8 digits under gtin13
			bool valid_length = keyword->digits == 8 ? length == 8
			                    : keyword->digits ? length >= 12 && length <= 14
			                                      : length == 8 || (length >= 12 && length <= 14);
			if (valid_length && !continues && GtinCheckDigitValid(text + digits_begin, length)) {
				string digits(text + digits_begin, length);
				hits.push_back({string(14 - length, '0') + digits, digits, keyword->key, kind, digits_begin});
			}
			if (!array) {
				break;
			}
			// The next array element: closing quote, comma, whitespace and an opening quote
			auto next = i;
			while (next < size && next - i < 8 && (text[next] == '"' || text[next] == '\\' || text[next] == ',' ||
			                                        IsSpace(text[next]))) {
				next++;
			}
			if (next == i || next >= size || !IsDigit(text[next]) || memchr(text + i, ',', next - i) == nullptr) {
				break;
			}
			i = next;
		}
		position = i - 1;
	}
	return hits;
}

vector<string> Gtins(const char *text, idx_t size) {
	vector<string> gtins;
	for (auto &hit : GtinHits(text, size)) {
		gtins.push_back(std::move(hit.gtin));
	}
	std::sort(gtins.begin(), gtins.end());
	gtins.erase(std::unique(gtins.begin(), gtins.end()), gtins.end());
	return gtins;
}

void GtinHitsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool constant = args.AllConstant();
	auto count = constant ? 1 : args.size();
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	UnifiedVectorFormat htmls;
	args.data[0].ToUnifiedFormat(htmls);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);
	auto &child_type = ListType::GetChildType(result.GetType());
	for (idx_t i = 0; i < count; i++) {
		auto index = htmls.sel->get_index(i);
		if (!htmls.validity.RowIsValid(index)) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		vector<Value> values;
		for (auto &hit : GtinHits(html_data[index].GetData(), html_data[index].GetSize())) {
			child_list_t<Value> fields = {{"gtin", Value(hit.gtin)},
			                              {"digits", Value(hit.digits)},
			                              {"key", Value(hit.key)},
			                              {"kind", Value(hit.kind)},
			                              {"offset", Value::UBIGINT(hit.offset)}};
			values.push_back(Value::STRUCT(std::move(fields)));
		}
		result.SetValue(i, Value::LIST(child_type, std::move(values)));
	}
}

//! The GTIN hit closest to an occurrence of `needle`, within 2000 bytes. Pages with many products usually keep the
//! product's data next to its id, like "itemsByPid":{"1002730":{..."eans":["6411923686860"]}}.
void GtinNearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	constexpr idx_t MAX_DISTANCE = 2000;
	bool constant = args.AllConstant();
	auto count = constant ? 1 : args.size();
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	UnifiedVectorFormat htmls, needles;
	args.data[0].ToUnifiedFormat(htmls);
	args.data[1].ToUnifiedFormat(needles);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);
	auto needle_data = UnifiedVectorFormat::GetData<string_t>(needles);
	for (idx_t i = 0; i < count; i++) {
		auto hi = htmls.sel->get_index(i), ni = needles.sel->get_index(i);
		if (!htmls.validity.RowIsValid(hi) || !needles.validity.RowIsValid(ni) || needle_data[ni].GetSize() < 3) {
			result.SetValue(i, Value());
			continue;
		}
		auto text = html_data[hi].GetData();
		auto size = html_data[hi].GetSize();
		auto needle = needle_data[ni].GetString();
		auto hits = GtinHits(text, size);
		idx_t best_distance = MAX_DISTANCE + 1;
		string best;
		for (idx_t position = 0; position + needle.size() <= size;) {
			auto found = static_cast<const char *>(memmem(text + position, size - position, needle.data(), needle.size()));
			if (!found) {
				break;
			}
			auto at = idx_t(found - text);
			// Only the id itself, not a part of a longer number
			bool whole = (at == 0 || !IsDigit(text[at - 1])) &&
			             (at + needle.size() >= size || !IsDigit(text[at + needle.size()]));
			if (whole) {
				for (auto &hit : hits) {
					auto distance = hit.offset > at ? hit.offset - at : at - hit.offset;
					if (distance < best_distance) {
						best_distance = distance;
						best = hit.gtin;
					}
				}
			}
			position = at + 1;
		}
		result.SetValue(i, best_distance <= MAX_DISTANCE ? Value(best) : Value());
	}
}

//! GS1 information of a GTIN: its 14 digit form, length, prefix and flags for codes which aren't globally unique
//! trade items: restricted in-store codes (prefix 20-29 and UPC 2/4), ISBN (978, 979), ISSN (977), coupons
//! (980-999) and variable measure GTIN-14 (indicator 9)
void GtinInfoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool constant = args.AllConstant();
	auto count = constant ? 1 : args.size();
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	UnifiedVectorFormat codes;
	args.data[0].ToUnifiedFormat(codes);
	auto code_data = UnifiedVectorFormat::GetData<string_t>(codes);
	for (idx_t i = 0; i < count; i++) {
		auto index = codes.sel->get_index(i);
		if (!codes.validity.RowIsValid(index)) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		auto code = code_data[index].GetString();
		bool digits = !code.empty();
		for (auto c : code) {
			digits = digits && IsDigit(c);
		}
		auto length = code.size();
		bool valid = digits && (length == 8 || (length >= 12 && length <= 14)) &&
		             GtinCheckDigitValid(code.data(), length);
		vector<Value> flags;
		string gtin14;
		string prefix;
		if (valid) {
			gtin14 = string(14 - length, '0') + code;
			// GS1 prefix of the GTIN-13 inside the GTIN-14
			prefix = gtin14.substr(1, 3);
			auto first = gtin14[0];
			if (first == '9') {
				flags.emplace_back("variable_measure");
			}
			if (length == 8) {
				if (code[0] == '0' || code[0] == '2') {
					flags.emplace_back("restricted");
				}
			} else if (prefix[0] == '2' || (prefix >= "020" && prefix <= "029") || (prefix >= "040" && prefix <= "049")) {
				// In-store and variable weight codes: EAN-13 200-299, UPC-A number systems 2 and 4
				flags.emplace_back("restricted");
			} else if (prefix >= "050" && prefix <= "059") {
				flags.emplace_back("coupon");
			} else if (prefix == "977") {
				flags.emplace_back("issn");
			} else if (prefix == "978" || prefix == "979") {
				flags.emplace_back("isbn");
			} else if (prefix >= "980") {
				flags.emplace_back("coupon");
			}
			if (std::all_of(code.begin(), code.end(), [&](char c) { return c == code[0]; })) {
				flags.emplace_back("repeated_digits");
			}
		} else {
			flags.emplace_back("invalid");
		}
		auto kind = !valid ? Value() : Value(length == 8 ? "GTIN-8" : length == 12 ? "GTIN-12" : length == 13 ? "GTIN-13" : "GTIN-14");
		child_list_t<Value> fields = {{"gtin14", valid ? Value(gtin14) : Value()},
		                              {"kind", kind},
		                              {"prefix", valid ? Value(prefix) : Value()},
		                              {"flags", Value::LIST(LogicalType::VARCHAR, std::move(flags))}};
		result.SetValue(i, Value::STRUCT(std::move(fields)));
	}
}

template <vector<string> (*SCAN)(const char *, idx_t)>
void ScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool constant = args.AllConstant();
	auto count = constant ? 1 : args.size();
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	UnifiedVectorFormat htmls;
	args.data[0].ToUnifiedFormat(htmls);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);
	for (idx_t i = 0; i < count; i++) {
		auto index = htmls.sel->get_index(i);
		if (!htmls.validity.RowIsValid(index)) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		vector<Value> values;
		for (auto &value : SCAN(html_data[index].GetData(), html_data[index].GetSize())) {
			values.emplace_back(MakeValidUtf8(std::move(value)));
		}
		result.SetValue(i, Value::LIST(LogicalType::VARCHAR, std::move(values)));
	}
}

} // namespace

void RegisterFastScanFunctions(ExtensionLoader &loader) {
	auto list = LogicalType::LIST(LogicalType::VARCHAR);
	loader.RegisterFunction(
	    ScalarFunction("html_json_ld_raw", {LogicalType::VARCHAR}, list, ScanFunction<JsonLdScripts>));
	loader.RegisterFunction(ScalarFunction("html_gtins", {LogicalType::VARCHAR}, list, ScanFunction<Gtins>));
	child_list_t<LogicalType> hit = {{"gtin", LogicalType::VARCHAR},
	                                 {"digits", LogicalType::VARCHAR},
	                                 {"key", LogicalType::VARCHAR},
	                                 {"kind", LogicalType::VARCHAR},
	                                 {"offset", LogicalType::UBIGINT}};
	loader.RegisterFunction(ScalarFunction("html_gtin_hits", {LogicalType::VARCHAR},
	                                       LogicalType::LIST(LogicalType::STRUCT(hit)), GtinHitsFunction));
	loader.RegisterFunction(ScalarFunction("html_gtin_near", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::VARCHAR, GtinNearFunction));
	child_list_t<LogicalType> info = {{"gtin14", LogicalType::VARCHAR},
	                                  {"kind", LogicalType::VARCHAR},
	                                  {"prefix", LogicalType::VARCHAR},
	                                  {"flags", LogicalType::LIST(LogicalType::VARCHAR)}};
	loader.RegisterFunction(
	    ScalarFunction("gtin_info", {LogicalType::VARCHAR}, LogicalType::STRUCT(info), GtinInfoFunction));
}

} // namespace duckdb

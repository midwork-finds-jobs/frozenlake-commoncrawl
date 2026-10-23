// HTML and embedded JavaScript extraction with CSS selectors, parsed with lexbor.
//
// Selectors take an optional scrapy style suffix which tells what to return from the matching elements:
//   'h1' or 'h1::text'      normalized text content (whitespace collapsed and trimmed)
//   'script::raw'           text content as is, for example JSON in <script> tags
//   'div.job::html'         inner HTML
//   'div.job::outer_html'   outer HTML
//   'a::attr(href)'         attribute value
//
//   html_select(html, selector)        -> VARCHAR[]  every match
//   html_select_first(html, selector)  -> VARCHAR    first match
//   html_extract(html, {name: selector, ...}) -> STRUCT(name VARCHAR, ...)  first match of each selector,
//                                                the document is parsed only once
//   html_text(html)                    -> VARCHAR    readable text without scripts, styles and markup
//   js_object(html, name)              -> VARCHAR    JSON object or array assigned to `name` in a script

#include "extract_extension.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <lexbor/css/css.h>
#include <lexbor/html/html.h>
#include <lexbor/html/tree/open_elements.h>
#include <lexbor/selectors/selectors.h>

namespace duckdb {

namespace {

enum class SelectMode { TEXT, RAW, HTML, OUTER_HTML, ATTRIBUTE };

struct SelectorSpec {
	string css;
	SelectMode mode = SelectMode::TEXT;
	string attribute;
};

SelectorSpec ParseSelectorSpec(const string &input) {
	SelectorSpec spec;
	spec.css = input;
	auto position = input.rfind("::");
	if (position == string::npos) {
		return spec;
	}
	auto suffix = StringUtil::Lower(input.substr(position + 2));
	auto css = input.substr(0, position);
	if (suffix == "text") {
		spec.mode = SelectMode::TEXT;
	} else if (suffix == "raw") {
		spec.mode = SelectMode::RAW;
	} else if (suffix == "html") {
		spec.mode = SelectMode::HTML;
	} else if (suffix == "outer_html") {
		spec.mode = SelectMode::OUTER_HTML;
	} else if (StringUtil::StartsWith(suffix, "attr(") && StringUtil::EndsWith(suffix, ")")) {
		spec.mode = SelectMode::ATTRIBUTE;
		spec.attribute = input.substr(position + 7, input.size() - position - 8);
		StringUtil::Trim(spec.attribute);
	} else {
		// A CSS pseudo-element such as ::before, leave the selector as it is
		return spec;
	}
	StringUtil::Trim(css);
	spec.css = css.empty() ? "*" : css;
	return spec;
}

bool IsSpace(unsigned char c) {
	return c == ' ' || c == '\n' || c == '\t' || c == '\r' || c == '\f' || c == '\v';
}

// Collapses whitespace including non-breaking spaces to single spaces and trims
void AppendNormalized(string &out, const char *data, idx_t length, bool &pending_space) {
	for (idx_t i = 0; i < length; i++) {
		unsigned char c = data[i];
		bool space = IsSpace(c);
		if (c == 0xC2 && i + 1 < length && (unsigned char)data[i + 1] == 0xA0) {
			space = true;
			i++;
		}
		if (space) {
			pending_space = !out.empty();
			continue;
		}
		if (pending_space) {
			out.push_back(' ');
			pending_space = false;
		}
		out.push_back(char(c));
	}
}

string Normalize(const char *data, idx_t length) {
	string out;
	bool pending_space = false;
	AppendNormalized(out, data, length, pending_space);
	return out;
}

// Owns one parsed document and the lexbor objects to query it
class HtmlDocument {
public:
	HtmlDocument() {
		document = lxb_html_document_create();
		parser = lxb_css_parser_create();
		memory = lxb_css_memory_create();
		css_selectors = lxb_css_selectors_create();
		selectors = lxb_selectors_create();
		if (!document || !parser || !memory || !css_selectors || !selectors ||
		    lxb_css_parser_init(parser, nullptr) != LXB_STATUS_OK || lxb_css_memory_init(memory, 128) != LXB_STATUS_OK ||
		    lxb_css_selectors_init(css_selectors) != LXB_STATUS_OK || lxb_selectors_init(selectors) != LXB_STATUS_OK) {
			Destroy();
			throw InternalException("failed to initialize lexbor");
		}
		// The compiled selector lists live in this memory until the document is destroyed
		lxb_css_parser_memory_set(parser, memory);
		lxb_css_parser_selectors_set(parser, css_selectors);
		// Every matching element only once even if it matches several selectors of a list
		lxb_selectors_opt_set(selectors, LXB_SELECTORS_OPT_MATCH_FIRST);
	}

	~HtmlDocument() {
		Destroy();
	}

	void Parse(const string_t &html) {
		lxb_html_document_clean(document);
		if (lxb_html_document_parse(document, const_data_ptr_cast(html.GetData()), html.GetSize()) !=
		    LXB_STATUS_OK) {
			throw InvalidInputException("failed to parse HTML");
		}
	}

	//! Appends the matches of the selector, up to limit
	void Select(const SelectorSpec &spec, idx_t limit, vector<string> &out) {
		auto list = Compile(spec.css);
		struct Context {
			const SelectorSpec *spec;
			idx_t limit;
			vector<string> *out;
			std::exception_ptr error;
		} context {&spec, limit, &out, nullptr};
		// C++ exceptions must not unwind through lexbor's C code
		auto callback = [](lxb_dom_node_t *node, lxb_css_selector_specificity_t, void *ctx) -> lxb_status_t {
			auto &context = *static_cast<Context *>(ctx);
			try {
				string value;
				if (Render(node, *context.spec, value)) {
					context.out->push_back(std::move(value));
				}
			} catch (...) {
				context.error = std::current_exception();
				return LXB_STATUS_ERROR;
			}
			return context.out->size() >= context.limit ? LXB_STATUS_STOP : LXB_STATUS_OK;
		};
		auto status = lxb_selectors_find(selectors, lxb_dom_interface_node(document), list, callback, &context);
		if (context.error) {
			std::rethrow_exception(context.error);
		}
		if (status != LXB_STATUS_OK && status != LXB_STATUS_STOP) {
			throw InvalidInputException("failed to match CSS selector \"%s\"", spec.css);
		}
	}

	//! Incremental parsing: the document grows with every chunk and can be queried in between
	void BeginChunks() {
		lxb_html_document_clean(document);
		if (lxb_html_document_parse_chunk_begin(document) != LXB_STATUS_OK) {
			throw InvalidInputException("failed to parse HTML");
		}
	}

	void ParseChunk(const char *data, idx_t size) {
		if (lxb_html_document_parse_chunk(document, const_data_ptr_cast(data), size) != LXB_STATUS_OK) {
			throw InvalidInputException("failed to parse HTML");
		}
	}

	void EndChunks() {
		lxb_html_document_parse_chunk_end(document);
	}

	//! Whether the first match of the selector exists and its element is closed, so that more HTML can't change it
	bool FirstMatchIsFinal(const SelectorSpec &spec) {
		auto list = Compile(spec.css);
		lxb_dom_node_t *first = nullptr;
		auto callback = [](lxb_dom_node_t *node, lxb_css_selector_specificity_t, void *ctx) -> lxb_status_t {
			*static_cast<lxb_dom_node_t **>(ctx) = node;
			return LXB_STATUS_STOP;
		};
		auto status = lxb_selectors_find(selectors, lxb_dom_interface_node(document), list, callback, &first);
		if ((status != LXB_STATUS_OK && status != LXB_STATUS_STOP) || !first) {
			return false;
		}
		if (spec.mode == SelectMode::ATTRIBUTE) {
			// Attributes are complete with the start tag
			return true;
		}
		auto parser = static_cast<lxb_html_parser_t *>(lxb_dom_interface_document(document)->parser);
		if (!parser || !parser->tree) {
			return false;
		}
		size_t index = 0;
		return !lxb_html_tree_open_elements_find_by_node(parser->tree, first, &index);
	}

	string Text() {
		string out;
		bool pending_space = false;
		AppendText(lxb_dom_interface_node(lxb_html_document_body_element(document)), out, pending_space);
		return out;
	}

private:
	lxb_html_document_t *document = nullptr;
	lxb_css_parser_t *parser = nullptr;
	lxb_css_memory_t *memory = nullptr;
	lxb_css_selectors_t *css_selectors = nullptr;
	lxb_selectors_t *selectors = nullptr;
	//! Selectors are usually constant so they are compiled only once per vector
	unordered_map<string, lxb_css_selector_list_t *> compiled;

	void Destroy() {
		lxb_selectors_destroy(selectors, true);
		lxb_css_parser_destroy(parser, true);
		lxb_css_selectors_destroy(css_selectors, true);
		lxb_css_memory_destroy(memory, true);
		lxb_html_document_destroy(document);
	}

	lxb_css_selector_list_t *Compile(const string &css) {
		auto entry = compiled.find(css);
		if (entry != compiled.end()) {
			return entry->second;
		}
		lxb_css_log_clean(lxb_css_parser_log(parser));
		auto list = lxb_css_selectors_parse(parser, const_data_ptr_cast(css.c_str()), css.size());
		// lexbor drops the invalid selectors of a list like "div, [bad" and only logs them
		if (!list || parser->status != LXB_STATUS_OK || lxb_css_log_length(lxb_css_parser_log(parser)) > 0) {
			throw InvalidInputException("invalid CSS selector \"%s\"", css);
		}
		compiled[css] = list;
		return list;
	}

	static bool Render(lxb_dom_node_t *node, const SelectorSpec &spec, string &value) {
		switch (spec.mode) {
		case SelectMode::TEXT:
		case SelectMode::RAW: {
			size_t length = 0;
			auto text = lxb_dom_node_text_content(node, &length);
			if (!text) {
				value.clear();
				return true;
			}
			value = spec.mode == SelectMode::TEXT ? Normalize(const_char_ptr_cast(text), length)
			                                      : string(const_char_ptr_cast(text), length);
			lxb_dom_document_destroy_text(node->owner_document, text);
			return true;
		}
		case SelectMode::HTML:
		case SelectMode::OUTER_HTML: {
			lexbor_str_t str = {nullptr, 0};
			auto status = spec.mode == SelectMode::HTML ? lxb_html_serialize_deep_str(node, &str)
			                                            : lxb_html_serialize_tree_str(node, &str);
			if (status != LXB_STATUS_OK) {
				return false;
			}
			value = str.data ? string(const_char_ptr_cast(str.data), str.length) : string();
			lexbor_str_destroy(&str, node->owner_document->text, false);
			return true;
		}
		case SelectMode::ATTRIBUTE: {
			if (node->type != LXB_DOM_NODE_TYPE_ELEMENT) {
				return false;
			}
			size_t length = 0;
			auto attribute = lxb_dom_element_get_attribute(lxb_dom_interface_element(node),
			                                               const_data_ptr_cast(spec.attribute.c_str()),
			                                               spec.attribute.size(), &length);
			if (!attribute) {
				return false;
			}
			value = string(const_char_ptr_cast(attribute), length);
			return true;
		}
		}
		return false;
	}

	static bool IsHidden(lxb_tag_id_t tag) {
		return tag == LXB_TAG_SCRIPT || tag == LXB_TAG_STYLE || tag == LXB_TAG_NOSCRIPT || tag == LXB_TAG_TEMPLATE ||
		       tag == LXB_TAG_SVG || tag == LXB_TAG_IFRAME || tag == LXB_TAG_HEAD;
	}

	static bool IsBlock(lxb_tag_id_t tag) {
		switch (tag) {
		case LXB_TAG_P:
		case LXB_TAG_DIV:
		case LXB_TAG_BR:
		case LXB_TAG_LI:
		case LXB_TAG_TR:
		case LXB_TAG_TD:
		case LXB_TAG_TH:
		case LXB_TAG_H1:
		case LXB_TAG_H2:
		case LXB_TAG_H3:
		case LXB_TAG_H4:
		case LXB_TAG_H5:
		case LXB_TAG_H6:
		case LXB_TAG_SECTION:
		case LXB_TAG_ARTICLE:
		case LXB_TAG_HEADER:
		case LXB_TAG_FOOTER:
		case LXB_TAG_NAV:
		case LXB_TAG_UL:
		case LXB_TAG_OL:
		case LXB_TAG_TABLE:
		case LXB_TAG_BLOCKQUOTE:
		case LXB_TAG_PRE:
		case LXB_TAG_DT:
		case LXB_TAG_DD:
		case LXB_TAG_ADDRESS:
			return true;
		default:
			return false;
		}
	}

	// Text of the visible elements. Block elements are separated with spaces so that words don't stick together.
	// Iterative so that deeply nested HTML can't overflow the stack.
	static void AppendText(lxb_dom_node_t *root, string &out, bool &pending_space) {
		if (!root) {
			return;
		}
		auto node = lxb_dom_node_first_child(root);
		while (node) {
			bool descend = false;
			if (node->type == LXB_DOM_NODE_TYPE_TEXT) {
				auto data = &lxb_dom_interface_character_data(node)->data;
				AppendNormalized(out, const_char_ptr_cast(data->data), data->length, pending_space);
			} else if (node->type == LXB_DOM_NODE_TYPE_ELEMENT) {
				auto tag = lxb_dom_node_tag_id(node);
				if (!IsHidden(tag)) {
					pending_space |= IsBlock(tag) && !out.empty();
					descend = lxb_dom_node_first_child(node) != nullptr;
				}
			}
			if (descend) {
				node = lxb_dom_node_first_child(node);
				continue;
			}
			// Next sibling, or the next sibling of the closest ancestor which has one
			while (node && node != root && !lxb_dom_node_next(node)) {
				node = lxb_dom_node_parent(node);
				if (node && node != root && node->type == LXB_DOM_NODE_TYPE_ELEMENT) {
					pending_space |= IsBlock(lxb_dom_node_tag_id(node)) && !out.empty();
				}
			}
			if (!node || node == root) {
				return;
			}
			if (node->type == LXB_DOM_NODE_TYPE_ELEMENT) {
				pending_space |= IsBlock(lxb_dom_node_tag_id(node)) && !out.empty();
			}
			node = lxb_dom_node_next(node);
		}
	}
};

} // namespace

struct HtmlStreamMatcher::State {
	HtmlDocument document;
	vector<SelectorSpec> pending;
};

unique_ptr<HtmlStreamMatcher> HtmlStreamMatcher::Create(const vector<string> &selectors) {
	auto matcher = unique_ptr<HtmlStreamMatcher>(new HtmlStreamMatcher());
	matcher->state = make_uniq<State>();
	for (auto &selector : selectors) {
		auto spec = ParseSelectorSpec(selector);
		// These look at elements after the match, a closed match isn't final for them
		auto css = StringUtil::Lower(spec.css);
		for (auto forward : {":has(", ":last", ":only", ":nth-last", ":empty"}) {
			if (css.find(forward) != string::npos) {
				return nullptr;
			}
		}
		matcher->state->pending.push_back(std::move(spec));
	}
	matcher->state->document.BeginChunks();
	return matcher;
}

HtmlStreamMatcher::HtmlStreamMatcher() = default;
HtmlStreamMatcher::~HtmlStreamMatcher() = default;

bool HtmlStreamMatcher::Feed(const char *data, idx_t size) {
	auto &state = *this->state;
	state.document.ParseChunk(data, size);
	for (idx_t i = 0; i < state.pending.size();) {
		if (state.document.FirstMatchIsFinal(state.pending[i])) {
			state.pending.erase(state.pending.begin() + i);
		} else {
			i++;
		}
	}
	return state.pending.empty();
}

namespace {

Value ToListValue(vector<string> &values) {
	vector<Value> list;
	list.reserve(values.size());
	for (auto &value : values) {
		list.emplace_back(std::move(value));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list));
}

template <bool FIRST>
void HtmlSelectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	UnifiedVectorFormat htmls, selectors;
	args.data[0].ToUnifiedFormat(htmls);
	args.data[1].ToUnifiedFormat(selectors);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);
	auto selector_data = UnifiedVectorFormat::GetData<string_t>(selectors);

	HtmlDocument document;
	unordered_map<string, SelectorSpec> specs;
	vector<string> matches;
	for (idx_t i = 0; i < count; i++) {
		auto hi = htmls.sel->get_index(i), si = selectors.sel->get_index(i);
		if (!htmls.validity.RowIsValid(hi) || !selectors.validity.RowIsValid(si)) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		auto selector = selector_data[si].GetString();
		auto spec = specs.find(selector);
		if (spec == specs.end()) {
			spec = specs.emplace(selector, ParseSelectorSpec(selector)).first;
		}
		document.Parse(html_data[hi]);
		matches.clear();
		document.Select(spec->second, FIRST ? 1 : NumericLimits<idx_t>::Maximum(), matches);
		if (FIRST) {
			result.SetValue(i, matches.empty() ? Value(LogicalType::VARCHAR) : Value(std::move(matches[0])));
		} else {
			result.SetValue(i, ToListValue(matches));
		}
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

unique_ptr<FunctionData> HtmlExtractBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &fields_type = arguments[1]->GetReturnType();
	if (fields_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("html_extract expects a struct of selectors, e.g. {title: 'h1', link: 'a::attr(href)'}");
	}
	child_list_t<LogicalType> children;
	for (auto &child : StructType::GetChildTypes(fields_type)) {
		if (child.second.id() != LogicalTypeId::VARCHAR) {
			throw BinderException("html_extract selectors must be VARCHAR, \"%s\" is %s", child.first,
			                      child.second.ToString());
		}
		children.emplace_back(child.first, LogicalType::VARCHAR);
	}
	bound_function.SetReturnType(LogicalType::STRUCT(children));
	return nullptr;
}

void HtmlExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	UnifiedVectorFormat htmls, fields;
	args.data[0].ToUnifiedFormat(htmls);
	args.data[1].ToUnifiedFormat(fields);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);

	auto &field_vectors = StructVector::GetEntries(args.data[1]);
	vector<UnifiedVectorFormat> field_formats(field_vectors.size());
	for (idx_t f = 0; f < field_vectors.size(); f++) {
		field_vectors[f].ToUnifiedFormat(field_formats[f]);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &outputs = StructVector::GetEntries(result);
	HtmlDocument document;
	unordered_map<string, SelectorSpec> specs;
	vector<string> matches;
	for (idx_t i = 0; i < count; i++) {
		auto hi = htmls.sel->get_index(i);
		if (!htmls.validity.RowIsValid(hi) || !fields.validity.RowIsValid(fields.sel->get_index(i))) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		document.Parse(html_data[hi]);
		for (idx_t f = 0; f < field_vectors.size(); f++) {
			auto &format = field_formats[f];
			auto fi = format.sel->get_index(i);
			if (!format.validity.RowIsValid(fi)) {
				FlatVector::SetNull(outputs[f], i, true);
				continue;
			}
			auto selector = UnifiedVectorFormat::GetData<string_t>(format)[fi].GetString();
			auto spec = specs.find(selector);
			if (spec == specs.end()) {
				spec = specs.emplace(selector, ParseSelectorSpec(selector)).first;
			}
			matches.clear();
			document.Select(spec->second, 1, matches);
			if (matches.empty()) {
				FlatVector::SetNull(outputs[f], i, true);
			} else {
				FlatVector::GetDataMutable<string_t>(outputs[f])[i] = StringVector::AddString(outputs[f], matches[0]);
			}
		}
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! html_query(html, kinds, selectors): the html_* calls of one query on the same HTML combined so that every
//! document is parsed only once, see optimizer.cpp. Kind is 'all' (html_select), 'first' (html_select_first) or
//! 'text' (html_text). The result is a struct with fields m0, m1, ... in the same order.
struct HtmlQueryData : public FunctionData {
	vector<string> kinds;
	vector<SelectorSpec> specs;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<HtmlQueryData>();
		copy->kinds = kinds;
		copy->specs = specs;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<HtmlQueryData>();
		if (kinds != other.kinds || specs.size() != other.specs.size()) {
			return false;
		}
		for (idx_t i = 0; i < specs.size(); i++) {
			if (specs[i].css != other.specs[i].css || specs[i].mode != other.specs[i].mode ||
			    specs[i].attribute != other.specs[i].attribute) {
				return false;
			}
		}
		return true;
	}
};

vector<string> ConstantStringList(ClientContext &context, Expression &expression, const char *name) {
	if (!expression.IsFoldable()) {
		throw BinderException("html_query %s must be constant", name);
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, expression);
	vector<string> result;
	for (auto &child : ListValue::GetChildren(value)) {
		result.push_back(child.IsNull() ? string() : StringValue::Get(child));
	}
	return result;
}

unique_ptr<FunctionData> HtmlQueryBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	auto data = make_uniq<HtmlQueryData>();
	data->kinds = ConstantStringList(input.GetClientContext(), *arguments[1], "kinds");
	auto selectors = ConstantStringList(input.GetClientContext(), *arguments[2], "selectors");
	if (selectors.size() != data->kinds.size()) {
		throw BinderException("html_query needs as many kinds as selectors");
	}
	child_list_t<LogicalType> children;
	for (idx_t i = 0; i < data->kinds.size(); i++) {
		auto &kind = data->kinds[i];
		if (kind != "all" && kind != "first" && kind != "text") {
			throw BinderException("unknown html_query kind \"%s\"", kind);
		}
		data->specs.push_back(ParseSelectorSpec(selectors[i]));
		children.emplace_back("m" + to_string(i),
		                      kind == "all" ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR);
	}
	input.GetBoundFunction().SetReturnType(LogicalType::STRUCT(children));
	return std::move(data);
}

void HtmlQueryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<HtmlQueryData>();
	idx_t count = args.size();
	UnifiedVectorFormat htmls;
	args.data[0].ToUnifiedFormat(htmls);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &outputs = StructVector::GetEntries(result);
	HtmlDocument document;
	vector<string> matches;
	for (idx_t i = 0; i < count; i++) {
		auto hi = htmls.sel->get_index(i);
		if (!htmls.validity.RowIsValid(hi)) {
			FlatVector::SetNull(result, i, true);
			for (auto &output : outputs) {
				FlatVector::SetNull(output, i, true);
			}
			continue;
		}
		document.Parse(html_data[hi]);
		for (idx_t m = 0; m < data.kinds.size(); m++) {
			auto &kind = data.kinds[m];
			if (kind == "text") {
				FlatVector::GetDataMutable<string_t>(outputs[m])[i] = StringVector::AddString(outputs[m], document.Text());
				continue;
			}
			matches.clear();
			document.Select(data.specs[m], kind == "first" ? 1 : NumericLimits<idx_t>::Maximum(), matches);
			if (kind == "all") {
				outputs[m].SetValue(i, ToListValue(matches));
			} else if (matches.empty()) {
				FlatVector::SetNull(outputs[m], i, true);
			} else {
				FlatVector::GetDataMutable<string_t>(outputs[m])[i] = StringVector::AddString(outputs[m], matches[0]);
			}
		}
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void HtmlTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	HtmlDocument document;
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t html) {
		document.Parse(html);
		return StringVector::AddString(result, document.Text());
	});
}

// Finds `name` followed by '=' or ':' and returns the balanced JSON object or array after it
bool FindJsObject(const string &html, const string &name, idx_t &start, idx_t &end) {
	auto skip_space = [&](idx_t p) {
		while (p < html.size() && IsSpace(html[p])) {
			p++;
		}
		return p;
	};
	auto is_identifier = [](char c) {
		return StringUtil::CharacterIsAlphaNumeric(c) || c == '_' || c == '$';
	};
	idx_t pos = 0;
	while ((pos = html.find(name, pos)) != string::npos) {
		// "data" must not match "metadata" or "data2"
		if ((pos > 0 && is_identifier(html[pos - 1])) ||
		    (pos + name.size() < html.size() && is_identifier(html[pos + name.size()]))) {
			pos = pos + 1;
			continue;
		}
		idx_t p = skip_space(pos + name.size());
		if (p >= html.size() || (html[p] != '=' && html[p] != ':')) {
			pos = pos + 1;
			continue;
		}
		p = skip_space(p + 1);
		if (p >= html.size() || (html[p] != '{' && html[p] != '[')) {
			pos = pos + 1;
			continue;
		}
		start = p;
		int depth = 0;
		char quote = 0;
		for (; p < html.size(); p++) {
			char c = html[p];
			if (quote) {
				if (c == '\\') {
					p++;
				} else if (c == quote) {
					quote = 0;
				}
			} else if (c == '"' || c == '\'' || c == '`') {
				quote = c;
			} else if (c == '{' || c == '[') {
				depth++;
			} else if (c == '}' || c == ']') {
				if (--depth == 0) {
					end = p + 1;
					return true;
				}
			}
		}
		pos = start;
	}
	return false;
}

void JsObjectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	UnifiedVectorFormat htmls, names;
	args.data[0].ToUnifiedFormat(htmls);
	args.data[1].ToUnifiedFormat(names);
	auto html_data = UnifiedVectorFormat::GetData<string_t>(htmls);
	auto name_data = UnifiedVectorFormat::GetData<string_t>(names);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto hi = htmls.sel->get_index(i), ni = names.sel->get_index(i);
		idx_t start, end;
		if (!htmls.validity.RowIsValid(hi) || !names.validity.RowIsValid(ni)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		auto html = html_data[hi].GetString();
		if (!FindJsObject(html, name_data[ni].GetString(), start, end)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		out[i] = StringVector::AddString(result, html.data() + start, end - start);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

void RegisterHtmlFunctions(ExtensionLoader &loader) {
	auto list = LogicalType::LIST(LogicalType::VARCHAR);
	// Invalid selectors and HTML parser failures throw
	ScalarFunction html_select("html_select", {LogicalType::VARCHAR, LogicalType::VARCHAR}, list,
	                           HtmlSelectFunction<false>);
	ScalarFunction html_select_first("html_select_first", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, HtmlSelectFunction<true>);
	ScalarFunction html_extract("html_extract", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalType::ANY,
	                            HtmlExtractFunction, HtmlExtractBind);
	ScalarFunction html_text("html_text", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HtmlTextFunction);
	ScalarFunction html_query("html_query",
	                          {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
	                           LogicalType::LIST(LogicalType::VARCHAR)},
	                          LogicalType::ANY, HtmlQueryFunction, HtmlQueryBind);
	for (auto function : {&html_select, &html_select_first, &html_extract, &html_text, &html_query}) {
		function->SetFallible();
		loader.RegisterFunction(*function);
	}
	loader.RegisterFunction(ScalarFunction("js_object", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::VARCHAR, JsObjectFunction));
}

} // namespace duckdb

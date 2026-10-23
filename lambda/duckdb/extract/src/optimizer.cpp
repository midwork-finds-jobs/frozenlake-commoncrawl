// Parses every HTML document only once even if a query uses many html_* functions on it:
//
//   SELECT html_select_first(body, 'h1'), html_select(body, 'a::attr(href)'), json_ld(body) FROM pages
//
// Before DuckDB's own optimizers run, the calls of a projection or filter which take the same HTML are rewritten to
// struct_extract(html_query(body, ['first', 'all', 'all'], ['h1', 'a::attr(href)', 'script[...]::raw']), 'm0') and
// so on. The html_query() calls are then identical so DuckDB's common subexpression optimizer evaluates them once.
// Macros like json_ld() are expanded to html_select() calls before this so they are combined too.
//
// Then, if the body of the records of warc_fetch() is only used for the first matches of selectors, like in
//
//   SELECT meta(record.body, 'og:title') FROM warc_fetch(...)
//
// warc_fetch() is told the selectors so it stops downloading a record as soon as they have all matched.

#include "extract_extension.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

struct HtmlCall {
	unique_ptr<Expression> *slot;
	//! 'all', 'first' or 'text' for each selector. html_extract has one per struct field.
	vector<string> kinds;
	vector<string> selectors;
	//! Field names of html_extract, empty for the other functions
	vector<string> fields;
	bool is_extract = false;
};

string FunctionName(const Expression &expression) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return string();
	}
	return expression.Cast<BoundFunctionExpression>().Function().GetName().GetIdentifierName();
}

bool IsHtmlFunction(const string &name) {
	return name == "html_select" || name == "html_select_first" || name == "html_text" || name == "html_extract";
}

bool ContainsHtmlCall(const Expression &expression) {
	if (IsHtmlFunction(FunctionName(expression))) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expression,
	                                      [&](const Expression &child) { found = found || ContainsHtmlCall(child); });
	return found;
}

bool UsesLambdaParameter(const Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_LAMBDA_REF) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expression,
	                                      [&](const Expression &child) { found = found || UsesLambdaParameter(child); });
	return found;
}

//! The constant VARCHAR value of a function argument. Macros like meta() build their selector with || which isn't
//! folded into a constant yet when this optimizer runs, so foldable expressions are evaluated here.
bool ConstantString(ClientContext &context, const Expression &expression, string &result) {
	Value folded;
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		if (!expression.IsFoldable() || !ExpressionExecutor::TryEvaluateScalar(context, expression, folded)) {
			return false;
		}
	} else {
		folded = expression.Cast<BoundConstantExpression>().GetValue();
	}
	auto &value = folded;
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	result = StringValue::Get(value);
	return true;
}

//! Fills the call if the expression is an html_* function which can be combined
bool ParseCall(ClientContext &context, unique_ptr<Expression> &slot, HtmlCall &call) {
	if (!IsHtmlFunction(FunctionName(*slot))) {
		return false;
	}
	auto name = FunctionName(*slot);
	auto &children = slot->Cast<BoundFunctionExpression>().GetChildren();
	// Rewriting a call whose HTML comes from another html_* call would invalidate the inner call
	if (children.empty() || UsesLambdaParameter(*children[0]) || ContainsHtmlCall(*children[0])) {
		return false;
	}
	call.slot = &slot;
	if (name == "html_text") {
		call.kinds = {"text"};
		call.selectors = {""};
		return true;
	}
	if (name == "html_extract") {
		// The struct of selectors is a constant struct_pack() or a constant
		auto &fields = children[1];
		if (!fields->IsFoldable() || fields->GetReturnType().id() != LogicalTypeId::STRUCT) {
			return false;
		}
		if (fields->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &value = fields->Cast<BoundConstantExpression>().GetValue();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_values = StructValue::GetChildren(value);
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (child_values[i].IsNull()) {
				return false;
			}
			call.fields.push_back(child_types[i].first.GetIdentifierName());
			call.kinds.push_back("first");
			call.selectors.push_back(StringValue::Get(child_values[i]));
		}
		call.is_extract = true;
		return !call.kinds.empty();
	}
	string selector;
	if (!ConstantString(context, *children[1], selector)) {
		return false;
	}
	call.kinds = {name == "html_select" ? "all" : "first"};
	call.selectors = {selector};
	return true;
}

void CollectCalls(ClientContext &context, unique_ptr<Expression> &expression, vector<HtmlCall> &calls) {
	HtmlCall call;
	if (ParseCall(context, expression, call)) {
		calls.push_back(std::move(call));
		return;
	}
	// Branches of CASE are evaluated only for some rows, combining them would run every selector on every row
	if (expression->GetExpressionClass() == ExpressionClass::BOUND_CASE) {
		return;
	}
	ExpressionIterator::EnumerateChildren(*expression,
	                                      [&](unique_ptr<Expression> &child) { CollectCalls(context, child, calls); });
}

Value StringList(const vector<string> &strings) {
	vector<Value> values;
	for (auto &string_value : strings) {
		values.emplace_back(string_value);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

unique_ptr<Expression> StructExtract(FunctionBinder &binder, const Expression &query, idx_t member) {
	vector<unique_ptr<Expression>> children;
	children.push_back(query.Copy());
	children.push_back(make_uniq<BoundConstantExpression>(Value("m" + to_string(member))));
	return binder.BindScalarFunction(DEFAULT_SCHEMA, "struct_extract", std::move(children));
}

void CombineCalls(ClientContext &context, vector<unique_ptr<Expression>> &expressions) {
	vector<HtmlCall> calls;
	for (auto &expression : expressions) {
		CollectCalls(context, expression, calls);
	}
	vector<bool> done(calls.size(), false);
	FunctionBinder binder(context);
	for (idx_t i = 0; i < calls.size(); i++) {
		if (done[i]) {
			continue;
		}
		auto &html = *(*calls[i].slot)->Cast<BoundFunctionExpression>().GetChildren()[0];
		vector<idx_t> group;
		for (idx_t j = i; j < calls.size(); j++) {
			// The slot of a call combined in an earlier group was replaced, by a CASE for html_extract()
			if (done[j]) {
				continue;
			}
			auto &other = *(*calls[j].slot)->Cast<BoundFunctionExpression>().GetChildren()[0];
			if (html.Equals(other)) {
				group.push_back(j);
				done[j] = true;
			}
		}
		idx_t selector_count = 0;
		for (auto j : group) {
			selector_count += calls[j].kinds.size();
		}
		if (selector_count < 2) {
			continue;
		}

		vector<string> kinds, selectors;
		for (auto j : group) {
			kinds.insert(kinds.end(), calls[j].kinds.begin(), calls[j].kinds.end());
			selectors.insert(selectors.end(), calls[j].selectors.begin(), calls[j].selectors.end());
		}
		vector<unique_ptr<Expression>> query_children;
		query_children.push_back(html.Copy());
		query_children.push_back(make_uniq<BoundConstantExpression>(StringList(kinds)));
		query_children.push_back(make_uniq<BoundConstantExpression>(StringList(selectors)));
		auto query = binder.BindScalarFunction(DEFAULT_SCHEMA, "html_query", std::move(query_children));

		// Members are numbered in group order
		idx_t member = 0;
		vector<idx_t> offsets;
		for (auto j : group) {
			offsets.push_back(member);
			member += calls[j].kinds.size();
		}
		for (idx_t g = 0; g < group.size(); g++) {
			auto &call = calls[group[g]];
			auto &slot = *call.slot;
			auto alias = slot->GetAlias();
			unique_ptr<Expression> replacement;
			if (call.is_extract) {
				vector<unique_ptr<Expression>> fields;
				for (idx_t f = 0; f < call.fields.size(); f++) {
					auto field = StructExtract(binder, *query, offsets[g] + f);
					field->SetAlias(Identifier(call.fields[f]));
					fields.push_back(std::move(field));
				}
				auto pack = binder.BindScalarFunction(DEFAULT_SCHEMA, "struct_pack", std::move(fields));
				// html_extract() of NULL is NULL, not a struct of NULLs
				auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
				is_null->GetChildrenMutable().push_back(query->Copy());
				auto null_struct = make_uniq<BoundConstantExpression>(Value(pack->GetReturnType()));
				replacement =
				    make_uniq<BoundCaseExpression>(std::move(is_null), std::move(null_struct), std::move(pack));
			} else {
				replacement = StructExtract(binder, *query, offsets[g]);
			}
			replacement->SetAlias(alias);
			slot = std::move(replacement);
		}
	}
}

void CombineInPlan(ClientContext &context, LogicalOperator &op) {
	for (auto &child : op.children) {
		CombineInPlan(context, *child);
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION || op.type == LogicalOperatorType::LOGICAL_FILTER) {
		CombineCalls(context, op.expressions);
	}
}

//! Finds the selectors whose first matches are all that a query needs from the bodies of warc_fetch() records
class StreamSelectorFinder {
public:
	explicit StreamSelectorFinder(ClientContext &context) : context(context) {
	}

	void Run(LogicalOperator &root) {
		Visit(root);
		// The output of the query is the full record
		PoisonOutput(root);
		for (auto &fetch : fetches) {
			// Selectors which can't be streamed would only add a second request for the rest of every record
			if (!fetch.poisoned && !fetch.selectors.empty() && HtmlStreamMatcher::Supports(fetch.selectors)) {
				SetWarcFetchStreamSelectors(*fetch.get->bind_data, fetch.selectors);
			}
		}
	}

private:
	struct Fetch {
		LogicalGet *get;
		vector<string> selectors;
		bool poisoned = false;
	};
	ClientContext &context;
	vector<Fetch> fetches;
	//! Bindings of a record column and the index of its warc_fetch() in fetches
	vector<std::pair<ColumnBinding, idx_t>> records;

	optional_idx RecordOf(const Expression &expression) {
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return optional_idx();
		}
		auto &binding = expression.Cast<BoundColumnRefExpression>().Binding();
		for (auto &record : records) {
			if (record.first == binding) {
				return record.second;
			}
		}
		return optional_idx();
	}

	void PoisonOutput(LogicalOperator &op) {
		for (auto &binding : op.GetColumnBindings()) {
			for (auto &record : records) {
				if (record.first == binding) {
					fetches[record.second].poisoned = true;
				}
			}
		}
	}

	//! The fetch whose body the expression is, as in record.body
	optional_idx BodyOf(const Expression &expression) {
		if (FunctionName(expression) != "struct_extract") {
			return optional_idx();
		}
		auto &children = expression.Cast<BoundFunctionExpression>().GetChildren();
		string field;
		if (children.size() != 2 || !ConstantString(context, *children[1], field) ||
		    !StringUtil::CIEquals(field, "body")) {
			return optional_idx();
		}
		return RecordOf(*children[0]);
	}

	//! The kinds and selectors of an html_* call
	bool ParseSelectors(unique_ptr<Expression> &slot, vector<string> &kinds, vector<string> &selectors) {
		auto name = FunctionName(*slot);
		if (name == "html_query") {
			auto &children = slot->Cast<BoundFunctionExpression>().GetChildren();
			for (idx_t i = 1; i <= 2; i++) {
				Value value;
				if (!children[i]->IsFoldable() || !ExpressionExecutor::TryEvaluateScalar(context, *children[i], value) ||
				    value.IsNull()) {
					return false;
				}
				auto &list = i == 1 ? kinds : selectors;
				for (auto &item : ListValue::GetChildren(value)) {
					if (item.IsNull()) {
						return false;
					}
					list.push_back(StringValue::Get(item));
				}
			}
			return kinds.size() == selectors.size();
		}
		HtmlCall call;
		if (!ParseCall(context, slot, call)) {
			return false;
		}
		kinds = std::move(call.kinds);
		selectors = std::move(call.selectors);
		return true;
	}

	void VisitExpression(unique_ptr<Expression> &expression) {
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		    !expression->Cast<BoundFunctionExpression>().GetChildren().empty()) {
			auto &children = expression->Cast<BoundFunctionExpression>().GetChildrenMutable();
			auto fetch = BodyOf(*children[0]);
			if (fetch.IsValid() && (FunctionName(*expression) == "html_query" ||
			                        IsHtmlFunction(FunctionName(*expression)))) {
				vector<string> kinds, selectors;
				bool first_only = ParseSelectors(expression, kinds, selectors);
				for (auto &kind : kinds) {
					first_only = first_only && kind == "first";
				}
				auto &entry = fetches[fetch.GetIndex()];
				if (!first_only) {
					entry.poisoned = true;
				}
				for (auto &selector : selectors) {
					if (std::find(entry.selectors.begin(), entry.selectors.end(), selector) == entry.selectors.end()) {
						entry.selectors.push_back(selector);
					}
				}
				for (idx_t i = 1; i < children.size(); i++) {
					VisitExpression(children[i]);
				}
				return;
			}
			// Other fields of the record are read from its headers
			if (FunctionName(*expression) == "struct_extract" && RecordOf(*children[0]).IsValid() &&
			    !BodyOf(*expression).IsValid()) {
				return;
			}
		}
		auto record = RecordOf(*expression);
		if (record.IsValid()) {
			fetches[record.GetIndex()].poisoned = true;
			return;
		}
		ExpressionIterator::EnumerateChildren(*expression,
		                                      [&](unique_ptr<Expression> &child) { VisitExpression(child); });
	}

	void Visit(LogicalOperator &op) {
		for (auto &child : op.children) {
			Visit(*child);
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			if (!op.children.empty()) {
				// A table in-out function reads all columns of its input, warc_fetch() also passes them through
				PoisonOutput(*op.children[0]);
			}
			if (IsWarcFetch(get.function.GetName().GetIdentifierName())) {
				fetches.push_back(Fetch {&get, {}, false});
				auto bindings = get.GetColumnBindings();
				auto &column_ids = get.GetColumnIds();
				for (idx_t i = 0; i < column_ids.size() && i < bindings.size(); i++) {
					auto column = get.projection_ids.empty() ? i : get.projection_ids[i].GetIndex();
					if (!column_ids[column].IsVirtualColumn() &&
					    get.GetColumnName(column_ids[column]).GetIdentifierName() == "record") {
						records.emplace_back(bindings[i], fetches.size() - 1);
					}
				}
			}
			return;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &projection = op.Cast<LogicalProjection>();
			for (idx_t i = 0; i < op.expressions.size(); i++) {
				auto record = RecordOf(*op.expressions[i]);
				if (record.IsValid()) {
					// Renamed, like the columns of a subquery
					records.emplace_back(ColumnBinding(projection.table_index, ProjectionIndex(i)),
					                     record.GetIndex());
				} else {
					VisitExpression(op.expressions[i]);
				}
			}
			return;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_SAMPLE:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		case LogicalOperatorType::LOGICAL_WINDOW:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			// These only read the columns their expressions refer to
			LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
				VisitExpression(*expression);
			});
			return;
		default:
			// Set operations, CTEs, inserts and so on read their inputs by position
			for (auto &child : op.children) {
				PoisonOutput(*child);
			}
			LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
				VisitExpression(*expression);
			});
			return;
		}
	}
};

void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	Value enabled;
	if (input.context.TryGetCurrentSetting("html_parse_once", enabled) && !enabled.IsNull() &&
	    !BooleanValue::Get(enabled)) {
		return;
	}
	CombineInPlan(input.context, *plan);
	Value stream;
	if (input.context.TryGetCurrentSetting("warc_fetch_stream", stream) && !stream.IsNull() &&
	    !BooleanValue::Get(stream)) {
		return;
	}
	StreamSelectorFinder(input.context).Run(*plan);
}

} // namespace

void RegisterHtmlOptimizer(ExtensionLoader &loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("html_parse_once", "Combine html_* calls on the same HTML to parse it only once",
	                        LogicalType::BOOLEAN, Value::BOOLEAN(true));
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("warc_fetch_stream",
	                        "Stop downloading a record in warc_fetch() once the selectors the query needs have matched",
	                        LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension extension;
	extension.pre_optimize_function = PreOptimize;
	OptimizerExtension::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()), extension);
}

} // namespace duckdb

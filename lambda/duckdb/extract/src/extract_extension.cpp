#define DUCKDB_EXTENSION_MAIN

#include "extract_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	RegisterWarcFunctions(loader);
	RegisterHtmlFunctions(loader);
	RegisterUrlFunctions(loader);
	RegisterCdxFunctions(loader);
	RegisterFastScanFunctions(loader);
	RegisterMacros(loader);
	RegisterMemoryGovernor(loader);
	RegisterHtmlOptimizer(loader);
}

void ExtractExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ExtractExtension::Name() {
	return "extract";
}

std::string ExtractExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(extract, loader) {
	duckdb::LoadInternal(loader);
}
}

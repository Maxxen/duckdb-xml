#define DUCKDB_EXTENSION_MAIN

#include "xml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "xlsx_reader.hpp"
#include "xml_reader.hpp"

namespace duckdb {

inline void XmlScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Xml " + name.GetString() + " 🐥");
		;
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto xml_scalar_function = ScalarFunction("xml", {LogicalType::VARCHAR}, LogicalType::VARCHAR, XmlScalarFun);
	ExtensionUtil::RegisterFunction(instance, xml_scalar_function);

	XLSXReader::Register(instance);
	XMLReader::Register(instance);
}

void XmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string XmlExtension::Name() {
	return "xml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void xml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *xml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

#pragma once
#include "duckdb.hpp"

namespace duckdb {


struct XLSXReader {
	static void Register(DatabaseInstance &db);
	static void RegisterCopy(DatabaseInstance &db);
};

} // namespace duckdb
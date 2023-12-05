#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct XMLReader {
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb
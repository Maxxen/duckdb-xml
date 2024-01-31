#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>

namespace duckdb {

enum class CellType {
	STRING,
	NUMBER,
	DATE,
	BOOLEAN,
	ERROR,
	SHARED_STRING,
};

struct CellTypes {
	static LogicalType GetType(CellType type);
	static CellType FromString(const char *str);
};

struct CellRef {
	static constexpr uint32_t MAX_ROWS = 1048576;
	static constexpr uint32_t MAX_COLS = 16384;

	uint32_t row;
	uint32_t col;

	explicit CellRef();
	explicit CellRef(uint32_t row, uint32_t col);
	explicit CellRef(const char *text);

	string ToString();
	string ColumnToString();
	string RowToString();
};

struct CellRange {
	CellRef start;
	CellRef end;

	explicit CellRange();
	explicit CellRange(CellRef start, CellRef end);
	explicit CellRange(const char *str);

	string ToString();
	bool Contains(const CellRef &cell);
	CellRef Normalize(const CellRef &cell);

	vector<CellRef> GetColumns();
	uint32_t GetWidth();
	uint32_t GetHeight();
};

} // namespace duckdb
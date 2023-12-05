#include "duckdb/common/types.hpp"

#include "xlsx_cell.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Cell Type
//------------------------------------------------------------------------------

LogicalType CellTypes::GetType(CellType type)  {
	switch(type) {
	case CellType::STRING:
	case CellType::SHARED_STRING:
		return LogicalType::VARCHAR;
	case CellType::NUMBER:
		return LogicalType::DOUBLE;
	case CellType::DATE:
		return LogicalType::DATE;
	case CellType::BOOLEAN:
		return LogicalType::BOOLEAN;
	case CellType::ERROR:
		return LogicalType::VARCHAR;
	default:
		throw NotImplementedException("Unknown cell type");
	}
}

CellType CellTypes::FromString(const char *str) {
	if(strcmp(str, "s") == 0) {
		return CellType::SHARED_STRING;
	} else if(strcmp(str, "str") == 0) {
		return CellType::STRING;
	} else if(strcmp(str, "n") == 0) {
		return CellType::NUMBER;
	} else if(strcmp(str, "b") == 0) {
		return CellType::BOOLEAN;
	} else if(strcmp(str, "e") == 0) {
		return CellType::ERROR;
	} else if(strcmp(str, "d") == 0) {
		return CellType::DATE;
	} else {
		throw InvalidInputException("Unknown cell type: %s", str);
	}
}

//------------------------------------------------------------------------------
// Cell Ref
//------------------------------------------------------------------------------

CellRef::CellRef() : row(0), col(0) {
}

CellRef::CellRef(uint32_t row, uint32_t col) : row(row), col(col) {
	if(row >= MAX_ROWS) {
		throw IOException("Row %d is out of range", row);
	}
	if(col >= MAX_COLS) {
		throw IOException("Column %d is out of range", col);
	}
}

CellRef::CellRef(const char *str) {
	D_ASSERT(str);
	// Convert the cell reference to a row and column
	// The cell reference is a string of the form "A1", "B2", etc.
	// The column is a base-26 number, where A=0, B=1, ..., Z=25, AA=26, AB=27, ..., AZ=51, BA=52, ..., ZZ=701, AAA=702, etc.
	// The row is a base-10 number, where 1=0, 2=1, 3=2, etc.
	col = 0;
	row = 0;
	auto c = *str++;
	while(c >= 'A' && c <= 'Z') {
		col *= 26;
		col += c - 'A';
		c = *str++;
	}
	while(c >= '0' && c <= '9') {
		row *= 10;
		row += c - '0';
		c = *str++;
	}
	row--; // Convert to 0-based
	str--;

	if(row >= MAX_ROWS) {
		throw IOException("Row %d is out of range", row);
	}

	if(col >= MAX_COLS) {
		throw IOException("Column %d is out of range", col);
	}
}

string CellRef::ToString() {
	string result;
	auto col_copy = col + 1;
	while(col_copy > 0) {
		auto remainder = col_copy % 26;
		col_copy /= 26;
		result += (char)('A' + remainder - 1);
	}
	std::reverse(result.begin(), result.end());
	result += std::to_string(row + 1);
	return result;
}

string CellRef::ColumnToString() {
	string result;
	auto col_copy = col + 1;
	while(col_copy > 0) {
		auto remainder = col_copy % 26;
		col_copy /= 26;
		result += (char)('A' + remainder - 1);
	}
	std::reverse(result.begin(), result.end());
	return result;
}

string CellRef::RowToString() {
	return std::to_string(row + 1);
}

//------------------------------------------------------------------------------
// Cell Range
//------------------------------------------------------------------------------

CellRange::CellRange() : start(), end() {
}

CellRange::CellRange(CellRef start, CellRef end) : start(start), end(end) {
}

CellRange::CellRange(const char *str) {
	D_ASSERT(str);
	auto ptr = const_cast<char*>(str);
	start = CellRef(ptr);
	while(*ptr != ':') {
		ptr++;
	}
	ptr++;
	end = CellRef(ptr);
}

string CellRange::ToString() {
	return start.ToString() + ":" + end.ToString();
}

bool CellRange::Contains(const CellRef &cell) {
	return cell.row >= start.row && cell.row <= end.row && cell.col >= start.col && cell.col <= end.col;
}

CellRef CellRange::Normalize(const CellRef &cell) {
	CellRef result;
	result.row = cell.row - start.row;
	result.col = cell.col - start.col;
	return result;
}

vector<CellRef> CellRange::GetColumns() {
	vector<CellRef> result;
	for(uint32_t i = start.col; i <= end.col; i++) {
		result.emplace_back(start.row, i);
	}
	return result;
}

uint32_t CellRange::GetWidth() {
	return end.col - start.col + 1;
}

uint32_t CellRange::GetHeight() {
	return end.row - start.row + 1;
}


//------------------------------------------------------------------------------
// Cell Info (TODO)
//------------------------------------------------------------------------------

/*
class CellInfo {
private:
	CellRef ref;
	CellType type;
	string value;
public:
	CellInfo(CellRef ref_p, CellType type_p, string value_p) : ref(ref_p), type(type_p), value(std::move(value_p)) { }

	const CellRef &GetRef() const {
		return ref;
	}

	const CellType &GetType() const {
		return type;
	}

	const string& GetText(const unordered_map<idx_t, string> &spare_string_table) const {
		if(type == CellType::SHARED_STRING) {
			auto idx = std::stoi(value);
			auto entry = spare_string_table.find(idx);
			if(entry == spare_string_table.end()) {
				throw IOException("Failed to find shared string with index %d", idx);
			}
			return entry->second;
		} else {
			return value;
		}
	}
};
 */




}
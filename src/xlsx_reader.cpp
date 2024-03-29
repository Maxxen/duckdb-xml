#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "expat.h"
#include "xml_state_machine.hpp"
#include "xlsx_reader.hpp"
#include "zip_archive.hpp"

namespace duckdb {

static inline bool Matches(const char *a, const char *b) {
	return strcmp(a, b) == 0;
}

template <class... ARGS>
static bool StringContainsAny(const char *str, ARGS &&...args) {
	for (auto &&substr : {args...}) {
		if (strstr(str, substr) != nullptr) {
			return true;
		}
	}
	return false;
}

static inline const char *GetAttribute(const char **atts, const char *name, const char *default_value) {
	for (int i = 0; atts[i]; i += 2) {
		if (strcmp(atts[i], name) == 0) {
			return atts[i + 1];
		}
	}
	return default_value;
}

enum class CellType {
	STRING,
	NUMBER,
	DATE,
	BOOLEAN,
	ERROR,
	SHARED_STRING,
};

struct CellTypes {
	static LogicalType ToLogicalType(CellType type) {
		switch (type) {
		case CellType::STRING:
		case CellType::SHARED_STRING:
			return LogicalType::VARCHAR;
		case CellType::NUMBER:
			return LogicalType::DOUBLE;
		case CellType::DATE:
			return LogicalType::TIMESTAMP;
		case CellType::BOOLEAN:
			return LogicalType::BOOLEAN;
		case CellType::ERROR:
			return LogicalType::VARCHAR;
		default:
			throw NotImplementedException("Unknown cell type");
		}
	}

	static CellType FromString(const char *str) {
		if (strcmp(str, "s") == 0) {
			return CellType::SHARED_STRING;
		} else if ((strcmp(str, "str") == 0) || (strcmp(str, "inlineStr") == 0)) {
			return CellType::STRING;
		} else if (strcmp(str, "n") == 0) {
			return CellType::NUMBER;
		} else if (strcmp(str, "b") == 0) {
			return CellType::BOOLEAN;
		} else if (strcmp(str, "e") == 0) {
			return CellType::ERROR;
		} else if (strcmp(str, "d") == 0) {
			return CellType::DATE;
		} else {
			throw InvalidInputException("Unknown cell type: %s", str);
		}
	}

	static CellType FromAttributes(const char **atts) {
		for (int i = 0; atts[i]; i += 2) {
			if (strcmp(atts[i], "t") == 0) {
				return CellTypes::FromString(atts[i + 1]);
			}
		}
		return CellType::NUMBER;
	}
};

static string ColumnIdxToExcelName(idx_t col) {
	string result;
	while (col > 0) {
		auto remainder = col % 26;
		col /= 26;
		if (remainder == 0) {
			remainder = 26;
			col--;
		}
		result = (char)('A' + remainder - 1) + result;
	}
	return result;
}

static idx_t TryGetColumnIndex(const char **atts) {
	for (int i = 0; atts[i]; i += 2) {
		if (strcmp(atts[i], "r") == 0) {
			auto cell_ref = atts[i + 1];
			idx_t value = 0;
			char c = *cell_ref++;
			while (c >= 'A' && c <= 'Z') {
				value = value * 26 + (c - 'A' + 1);
				c = *cell_ref++;
			}
			return value - 1; // 0 indexed
		}
	}
	throw IOException("Failed to find column index, found no 'r' attribute");
}

static idx_t TryGetRowIdx(const char **atts) {
	for (int i = 0; atts[i]; i += 2) {
		if (strcmp(atts[i], "r") == 0) {
			idx_t value = std::atoi(atts[i + 1]);
			return value - 1; // 0 indexed
		}
	}
	throw IOException("Failed to find column index, found no 'r' attribute");
}

//------------------------------------------------------------------------------
// XLSX Parsers
//------------------------------------------------------------------------------

class XLSXStyleSheetParser : public XMLStateMachine<XLSXStyleSheetParser> {
	enum class State { START, STYLESHEET, NUMFMTS, NUMFMT, CELLXFS, XF } state = State::START;

	unordered_map<int, LogicalType> format_map;

public:
	vector<LogicalType> style_formats;

	void OnStartElement(const char *name, const char **atts) {
		switch (state) {
		case State::START:
			if (Matches(name, "styleSheet")) {
				state = State::STYLESHEET;
				return;
			}
			break;
		case State::STYLESHEET:
			if (Matches(name, "numFmts")) {
				state = State::NUMFMTS;
				return;
			} else if (Matches(name, "cellXfs")) {
				state = State::CELLXFS;
				return;
			}
			break;
		case State::NUMFMTS: {
			if (Matches(name, "numFmt")) {
				state = State::NUMFMT;

				auto id = std::atoi(GetAttribute(atts, "numFmtId", "-1"));
				auto format = GetAttribute(atts, "formatCode", nullptr);
				// Format id starts at 164
				if (id > 163 && format != nullptr) {
					// Try to detect if the format is a date or time
					auto has_date_part = StringContainsAny(format, "DD", "dd", "YY", "yy");
					auto has_time_part = StringContainsAny(format, "HH", "hh");

					if (has_date_part && has_time_part) {
						format_map.emplace(id, LogicalType::TIMESTAMP);
					} else if (has_date_part) {
						format_map.emplace(id, LogicalType::DATE);
					} else if (has_time_part) {
						format_map.emplace(id, LogicalType::TIME);
					} else {
						format_map.emplace(id, LogicalType::VARCHAR); // Or double?
					}
				}
			}
		} break;
		case State::CELLXFS: {
			if (Matches(name, "xf")) {
				state = State::XF;

				auto id = std::atoi(GetAttribute(atts, "numFmtId", "-1"));
				// TODO: This should probably be varchar, we need to check if we should apply the number format or not
				LogicalType type = LogicalType::DOUBLE;
				if (id < 164) {
					// Special cases
					if (id >= 14 && id <= 17) {
						type = LogicalType::DATE;
					} else if (id >= 18 && id <= 21) {
						type = LogicalType::TIME;
					} else if (id == 22) {
						type = LogicalType::TIMESTAMP;
					}
				} else {
					// Look up the ID in the format map
					auto it = format_map.find(id);
					if (it != format_map.end()) {
						type = it->second;
					}
				}
				style_formats.push_back(type);
			}
		} break;
		default:
			break;
		}
	}

	void OnEndElement(const char *name) {
		switch (state) {
		case State::STYLESHEET:
			if (Matches(name, "styleSheet")) {
				state = State::START;
				Stop();
				return;
			}
			break;
		case State::NUMFMTS:
			if (Matches(name, "numFmts")) {
				state = State::STYLESHEET;
				return;
			}
			break;
		case State::NUMFMT:
			if (Matches(name, "numFmt")) {
				state = State::NUMFMTS;
				return;
			}
			break;
		case State::CELLXFS:
			if (Matches(name, "cellXfs")) {
				state = State::STYLESHEET;
				return;
			}
			break;
		case State::XF:
			if (Matches(name, "xf")) {
				state = State::CELLXFS;
				return;
			}
			break;
		default:
			break;
		}
	}
	void OnCharacterData(const char *s, int len) {
	}
};

class StyleSheet {
	vector<LogicalType> formats;

public:
	explicit StyleSheet(vector<LogicalType> formats_p) : formats(std::move(formats_p)) {
	}
	explicit StyleSheet() {
	}
	LogicalType GetStyleFormat(idx_t idx) const {
		if (idx >= formats.size()) {
			return LogicalType::DOUBLE;
		}
		return formats[idx];
	}
};

class XLSXWorkBookParser : public XMLStateMachine<XLSXWorkBookParser> {
private:
	bool in_workbook = false;
	bool in_sheets = false;
	bool in_sheet = false;

public:
	vector<std::pair<string, idx_t>> sheets;

	void OnStartElement(const char *name, const char **atts) {
		if (!in_workbook && Matches(name, "workbook")) {
			in_workbook = true;
			return;
		}
		if (in_workbook && !in_sheets && Matches(name, "sheets")) {
			in_sheets = true;
			return;
		}
		if (in_workbook && in_sheets && !in_sheet && Matches(name, "sheet")) {
			in_sheet = true;
			string sheet_name;
			bool found_id = false;
			idx_t sheet_id;
			for (int i = 0; atts[i]; i += 2) {
				if (strcmp(atts[i], "name") == 0) {
					sheet_name = string(atts[i + 1]);
				} else if (strcmp(atts[i], "sheetId") == 0) {
					sheet_id = atoi(atts[i + 1]);
					found_id = true;
				}
			}

			if (found_id && !sheet_name.empty()) {
				sheets.emplace_back(sheet_name, sheet_id);
			}
		}
	}

	void OnEndElement(const char *name) {
		if (in_workbook && in_sheets && in_sheet && Matches(name, "sheet")) {
			in_sheet = false;
			return;
		}
		if (in_workbook && in_sheets && Matches(name, "sheets")) {
			in_sheets = false;
			return;
		}
		if (in_workbook && Matches(name, "workbook")) {
			in_workbook = false;
			Stop();
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
	}
};

enum class XLSXHeaderMode { ALWAYS, NEVER, AUTO };

class XLSXSheetSniffer : public XMLStateMachine<XLSXSheetSniffer> {
private:
	enum class State { START, ROW, COLUMN, VALUE, INLINE_STR, INLINE_STR_VALUE } state = State::START;

	bool row_has_any_data = false;

	idx_t current_row = 0;
	idx_t current_col = 0;

public:
	// Either given by the "skip" parameter, or autodetected based on the first row with data
	bool has_start_row = true;
	idx_t start_row = 0;

	// The maximum "width" of the sheet
	idx_t end_column = 0;

	// The row of the first "row" tag in the sheet. Not necessarily the first row with data.
	// We use this to detect how many "empty" rows we need to pad in the beginning
	bool found_row = false;
	idx_t first_row = 0;

	XLSXHeaderMode header_mode = XLSXHeaderMode::AUTO;
	bool found_header = false;

	// The data of the header
	vector<string> header_data;
	vector<LogicalType> header_types;
	vector<CellType> header_cells;

	// The data of the first row (after the header, if present)
	vector<string> row_data;
	vector<LogicalType> row_types;
	vector<CellType> row_cells;

	const StyleSheet &style_sheet;

	explicit XLSXSheetSniffer(const StyleSheet &style_sheet_p) : style_sheet(style_sheet_p) {
	}

	void OnStartElement(const char *name, const char **atts) {
		switch (state) {
		case State::START: {
			if (Matches(name, "row")) {
				state = State::ROW;
				current_row = TryGetRowIdx(atts);

				if (!found_row) {
					found_row = true;
					first_row = current_row;
				}
				row_has_any_data = false;
			}
		} break;
		case State::ROW: {
			if (Matches(name, "c")) {
				state = State::COLUMN;
				current_col = TryGetColumnIndex(atts);
				if (current_col >= end_column) {
					end_column = current_col;
					row_types.resize(current_col + 1, LogicalType::DOUBLE);
					row_cells.resize(current_col + 1, CellType::NUMBER);
					row_data.resize(current_col + 1, "");
				}
				auto cell_type = CellTypes::FromAttributes(atts);
				row_cells[current_col] = cell_type;
				row_types[current_col] = CellTypes::ToLogicalType(cell_type);

				// Check if we have a number format that overrides the type
				// auto style_idx = std::atoi(GetAttribute(atts, "s", "-1"));
				//if (style_idx > 1) { // TODO: Should be >= 0
				//	row_types[current_col] = style_sheet.GetStyleFormat(style_idx);
				//}
			}
		} break;
		case State::COLUMN: {
			if (Matches(name, "v")) {
				state = State::VALUE;
				EnableTextParsing(true);
			} else if (Matches(name, "is")) {
				state = State::INLINE_STR;
			}
		} break;
		case State::INLINE_STR: {
			if (Matches(name, "t")) {
				state = State::INLINE_STR_VALUE;
				EnableTextParsing(true);
			}
		} break;
		default:
			break;
		}
	}

	void OnEndElement(const char *name) {
		switch (state) {
		case State::ROW: {
			if (Matches(name, "row")) {
				state = State::START;

				// If we dont have a start row set, we set it to the first row that has any data
				if (!has_start_row && row_has_any_data) {
					has_start_row = true;
					start_row = current_row;
				}

				if (start_row < first_row) {
					// There is going to be empty padding rows, so there cant be a header
					if (header_mode == XLSXHeaderMode::ALWAYS) {
						// still, increment the start row
						start_row++;
					}
					Stop();
					return;
				}

				if (has_start_row && current_row >= start_row) {
					// Alright, we found one row with data. Should we continue?
					if (found_header || header_mode == XLSXHeaderMode::NEVER) {
						// No, we're done. We either found a header or we only want the data
						Stop();
						return;
					} else if (header_mode == XLSXHeaderMode::ALWAYS) {
						// Yes, we always read the first row as the header, so now we want the data
						found_header = true;
						header_data = row_data;
						header_types = row_types;
						header_cells = row_cells;
						start_row++;
					} else if (header_mode == XLSXHeaderMode::AUTO) {
						// Maybe, try to automatically detect if the first row is a header
						// We do this by checking if the first row is all strings (or empty)
						// If it is, we treat it as a header and continue, otherwise we stop
						bool all_strings = true;
						for (idx_t i = 0; i < row_cells.size(); i++) {
							auto &type = row_cells[i];
							bool is_empty = row_data[i].empty();
							if ((type != CellType::STRING && type != CellType::SHARED_STRING) || is_empty) {
								all_strings = false;
								break;
							}
						}
						if (all_strings) {
							// All are strings! Assume this is a header and continue
							found_header = true;
							header_data = row_data;
							header_types = row_types;
							header_cells = row_cells;
							start_row++;
						} else {
							// Some are not strings... this is probably data. Let's stop here.
							found_header = false;
							Stop();
							return;
						}
					}
				}

				// Prepare for reading another row.
				row_data.clear();
				row_data.resize(end_column + 1, "");
				row_types.clear();
				row_types.resize(end_column + 1, LogicalType::DOUBLE);
				row_cells.clear();
				row_cells.resize(end_column + 1, CellType::NUMBER);
				return;
			}
		} break;
		case State::COLUMN: {
			if (Matches(name, "c")) {
				state = State::ROW;
			}
		} break;
		case State::VALUE: {
			if (Matches(name, "v")) {
				state = State::COLUMN;
				EnableTextParsing(false);
			}
		} break;
		case State::INLINE_STR: {
			if (Matches(name, "is")) {
				state = State::COLUMN;
			}
		} break;
		case State::INLINE_STR_VALUE: {
			if (Matches(name, "t")) {
				state = State::INLINE_STR;
				EnableTextParsing(false);
			}
		} break;
		default:
			break;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (state == State::VALUE || state == State::INLINE_STR_VALUE) {
			row_has_any_data = true;
			row_data[current_col].append(s, len);
		}
	}
};

class XLSXSparseStringTableParser : public XMLStateMachine<XLSXSparseStringTableParser> {
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	string current_string;

public:
	// The indices of the strings we want to fetch
	unordered_set<idx_t> requested_string_indices;

	// The resulting strings we fetched
	unordered_map<idx_t, string> string_table;

	void OnStartElement(const char *name, const char **atts) {
		// TODO: Harden this to only match the t tag in the right namespace
		if (Matches(name, "t")) {
			in_t_tag = true;
			EnableTextParsing(true);
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (Matches(name, "t")) {
			in_t_tag = false;
			EnableTextParsing(false);

			if (requested_string_indices.find(current_string_idx) != requested_string_indices.end()) {
				// We care about this string
				string_table.emplace(current_string_idx, current_string);
				requested_string_indices.erase(current_string_idx);
				if (requested_string_indices.empty()) {
					// We're done
					Stop();
				}
			}
			current_string.clear();
			current_string_idx++;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_t_tag) {
			current_string.append(s, len);
		}
	}
};

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct XLSXBindInfo : public TableFunctionData {
	// The name of the file to read
	string file_name;
	// The name of the sheet to read
	string sheet_name;
	// Whether or not to treat all columns as VARCHAR
	bool all_varchar = false;

	// The index of the sheet in the archive
	int sheet_file_idx = -1;
	// The index of the string dictionary in the archive
	int string_dict_idx = -1;

	// The first row is a header
	bool first_row_is_header = false;
	// The number of rows to skip
	idx_t start_row = 0;
	// The total amount of columns read
	idx_t col_count = 0;
	// The empty rows to pad in the beginning (if any)
	idx_t initial_empty_rows = 0;

	// The style sheet
	unique_ptr<StyleSheet> style_sheet;

	// Cell types
	vector<CellType> cell_types;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XLSXBindInfo>();

	// first parameter is the file name
	result->file_name = input.inputs[0].GetValue<string>();

	bool has_first_row_is_header = false;
	bool first_row_is_header = false;
	bool has_initial_rows_to_skip = false;
	idx_t initial_rows_to_skip = 0;

	for (auto &kv : input.named_parameters) {
		auto &key = kv.first;
		auto &value = kv.second;

		if (key == "sheet") {
			result->sheet_name = value.GetValue<string>();
		} else if (key == "header") {
			has_first_row_is_header = true;
			first_row_is_header = value.GetValue<bool>();
		} else if (key == "skip") {
			has_initial_rows_to_skip = true;
			initial_rows_to_skip = value.GetValue<int>();
		} else if (key == "all_varchar") {
			result->all_varchar = value.GetValue<bool>();
		} else {
			throw NotImplementedException("Unknown parameter '%s' for function read_xlsx", key);
		}
	}

	// Open the file
	auto &fs = FileSystem::GetFileSystem(context);
	auto archive = ZipArchiveFileHandle::Open(fs, result->file_name);

	// Parse the workbook
	auto workbook_stream = archive->Extract("xl/workbook.xml");
	XLSXWorkBookParser workbook_parser;
	workbook_parser.ParseUntilEnd(workbook_stream);

	// Are there any sheets?
	if (workbook_parser.sheets.empty()) {
		throw IOException("Failed to find any sheets in file '%s'", result->file_name);
	}

	// Look for the sheet, if none is specified, use first one
	idx_t sheet_id = 0;
	if (!result->sheet_name.empty()) {
		// Check if the sheet is in the workbook
		bool found_sheet = false;
		for (auto &sheet : workbook_parser.sheets) {
			if (sheet.first == result->sheet_name) {
				sheet_id = sheet.second;
				found_sheet = true;
				break;
			}
		}
		if (!found_sheet) {
			// Could not find the sheet in the workbook
			vector<string> sheet_names;
			for (auto &sheet : workbook_parser.sheets) {
				sheet_names.push_back(sheet.first);
			}
			auto candidate_msg = StringUtil::CandidatesErrorMessage(sheet_names, result->sheet_name, "Suggestions");
			throw IOException("Failed to find sheet '%s' in file \n%s", result->sheet_name, candidate_msg);
		}
	} else {
		// Pick first sheet available
		auto &first_sheet = workbook_parser.sheets.front();
		result->sheet_name = first_sheet.first;
		sheet_id = first_sheet.second;
	}

	// The "sheet id" is not the same as the file index in the archive, so we have to look it up here
	// Look for the requested sheet in the archive
	auto target_path = StringUtil::Format("xl/worksheets/sheet%d.xml", sheet_id);
	// If this succeeds, we set the result sheet_file_idx
	if (!archive->TryGetEntryIndexByName(target_path, result->sheet_file_idx)) {
		throw IOException("Failed to find sheet with id '%d'", sheet_id);
	}

	D_ASSERT(result->sheet_file_idx >= 0);
	D_ASSERT(!result->sheet_name.empty());

	// Look for the string dictionary in the archive
	archive->TryGetEntryIndexByName("xl/sharedStrings.xml", result->string_dict_idx);

	// Also look for a style sheet, and if it exists, parse it immediately
	int style_sheet_idx = -1;
	if (archive->TryGetEntryIndexByName("xl/styles.xml", style_sheet_idx)) {
		auto style_sheet_stream = archive->Extract(style_sheet_idx);
		XLSXStyleSheetParser style_sheet_parser;
		style_sheet_parser.ParseUntilEnd(style_sheet_stream);
		result->style_sheet = make_uniq<StyleSheet>(std::move(style_sheet_parser.style_formats));
	} else {
		// Empty style sheet
		result->style_sheet = make_uniq<StyleSheet>();
	}

	// Now we can parse the sheet
	XLSXSheetSniffer sniffer(*result->style_sheet);
	sniffer.has_start_row = has_initial_rows_to_skip;
	sniffer.start_row = initial_rows_to_skip;
	sniffer.header_mode = has_first_row_is_header
	                          ? (first_row_is_header ? XLSXHeaderMode::ALWAYS : XLSXHeaderMode::NEVER)
	                          : XLSXHeaderMode::AUTO;

	// Execute sniffer
	auto sheet_stream = archive->Extract(result->sheet_file_idx);
	sniffer.ParseUntilEnd(sheet_stream);

	if (sniffer.row_types.empty()) {
		throw IOException("Failed to find any data in sheet '%s'", result->sheet_name);
	}

	// Set based on the sniffer info
	result->first_row_is_header = sniffer.found_header;
	result->start_row = sniffer.start_row;
	result->col_count = sniffer.end_column + 1; // We start at col 0. If we end at 2, we have 3 columns
	result->initial_empty_rows = sniffer.start_row > sniffer.first_row ? 0 : (sniffer.first_row - sniffer.start_row);
	result->cell_types = sniffer.row_cells;

	// Set the return types
	for (idx_t i = 0; i < sniffer.row_types.size(); i++) {
		if (result->all_varchar) {
			return_types.push_back(LogicalType::VARCHAR);
		} else {
			// Special case: treat empty rows as VARCHAR instead of defaulting to DOUBLE
			if (sniffer.row_data[i].empty()) {
				return_types.push_back(LogicalType::VARCHAR);
			} else {
				return_types.push_back(sniffer.row_types[i]);
			}
		}
	}

	// Now set the names.
	if (!result->first_row_is_header) {
		// No header, just give them the standard excel names
		for (idx_t i = 0; i < sniffer.row_types.size(); i++) {
			names.push_back(ColumnIdxToExcelName(i + 1)); // 1 indexed
		}
	} else {
		// If any names contain shared strings, we do a sparse scan of the string dictionary to find them.
		// Is there any shared strings in the header?
		vector<idx_t> shared_string_indices;
		for (idx_t i = 0; i < sniffer.header_cells.size(); i++) {
			if (sniffer.header_cells[i] == CellType::SHARED_STRING) {
				shared_string_indices.push_back(std::stoi(sniffer.header_data[i]));
			}
		}
		if (shared_string_indices.empty()) {
			// No shared strings, all the names are already in the header
			names = sniffer.header_data;
		} else {
			// We need a string dictionary to find the shared strings
			if (result->string_dict_idx < 0) {
				throw IOException("Failed to find string dictionary in file '%s'", result->file_name);
			}
			// Scan the string dictionary for the shared strings
			auto string_dict_stream = archive->Extract(result->string_dict_idx);
			XLSXSparseStringTableParser parser;
			parser.requested_string_indices.insert(shared_string_indices.begin(), shared_string_indices.end());
			parser.ParseUntilEnd(string_dict_stream);

			// Now we can set the names
			for (idx_t i = 0; i < sniffer.header_cells.size(); i++) {
				if (sniffer.header_cells[i] == CellType::SHARED_STRING) {
					auto idx = std::stoi(sniffer.header_data[i]);
					auto entry = parser.string_table.find(idx);
					if (entry == parser.string_table.end()) {
						throw IOException("Failed to find shared string with index %d", idx);
					}
					names.push_back(entry->second);
				} else {
					names.push_back(sniffer.header_data[i]);
				}
			}
		}

		// Deduplicate names if necessary
		for (idx_t i = 0; i < names.size(); i++) {
			if (names[i].empty()) {
				names[i] = "col" + std::to_string(i);
			}
			for (idx_t j = 0; j < i; j++) {
				if (names[i] == names[j]) {
					names[i] += "_" + std::to_string(i);
					break;
				}
			}
		}
	}

	// Now we can deal with the types
	auto types_size = return_types.size();
	auto names_size = names.size();

	// TODO: Can we have more types than names?
	if (types_size < names_size) {
		// We have more names than types, so we need to add types
		for (auto i = types_size; i < names_size; i++) {
			return_types.push_back(LogicalType::VARCHAR);
		}
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

class DenseStringTable {
	vector<string_t> strings;

public:
	explicit DenseStringTable(Allocator &allocator_p) : allocator(allocator_p) {
	}

	ArenaAllocator allocator;

	void AddString(const char *str, idx_t len) {
		auto ptr = allocator.Allocate(len);
		memcpy(ptr, str, len);
		strings.emplace_back(const_char_ptr_cast(ptr), static_cast<uint32_t>(len));
	}

	const string_t &GetString(idx_t idx) const {
		if (idx > strings.size()) {
			throw IOException("Failed to find shared string with index %d", idx);
		}
		return strings[idx];
	}

	void Reserve(idx_t count) {
		strings.reserve(count);
	}
};

class XLSXDenseStringTableParser : public XMLStateMachine<XLSXDenseStringTableParser> {
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	vector<char> current_string;
	DenseStringTable &string_table;

public:
	explicit XLSXDenseStringTableParser(DenseStringTable &string_table_p) : string_table(string_table_p) {
	}

	void OnStartElement(const char *name, const char **atts) {
		// TODO: Harden this to only match the t tag in the right namespace
		if (strcmp(name, "t") == 0) {
			in_t_tag = true;
			EnableTextParsing(true);
			return;
		}

		if (strcmp(name, "sst") == 0) {
			// Optimization: look for the "uniqueCount" attribute value
			// and reserve space for that many strings
			for (int i = 0; atts[i]; i += 2) {
				if ((strcmp(atts[i], "uniqueCount") == 0)) {
					// Reserve space for the strings
					auto count = atoi(atts[i + 1]);
					string_table.Reserve(count);
					return;
				}
			}
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (strcmp(name, "t") == 0) {
			in_t_tag = false;
			EnableTextParsing(false);
			string_table.AddString(current_string.data(), current_string.size());
			current_string.clear();
			current_string_idx++;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_t_tag) {
			current_string.insert(current_string.end(), s, s + len);
		}
	}
};

class XLSXSheetReader : public XMLStateMachine<XLSXSheetReader> {
private:
	bool in_row = false;
	bool in_col = false;
	bool in_col_range = false;
	bool in_row_range = false;
	bool in_val = false;
	bool in_is = false;
	bool in_t = false;

	const DenseStringTable &string_table;

	idx_t start_row = 0;
	idx_t current_row = 0;
	idx_t current_col = 0;
	idx_t col_count = 0;

	CellType current_cell_type;
	string current_cell_data;
	vector<bool> current_validity;

	// Reverse projection map, mapping column index to output index (or -1 if not in output)
	vector<idx_t> projection_map;

public:
	idx_t current_output_row = 0;
	DataChunk payload_chunk;

	explicit XLSXSheetReader(ClientContext &ctx, const DenseStringTable &string_table_p, idx_t start_row_p,
	                         const vector<idx_t> &projection_map_p, idx_t output_column_count)
	    : string_table(string_table_p), start_row(start_row_p), col_count(projection_map_p.size()),
	      current_validity(projection_map_p.size(), false), projection_map(projection_map_p) {

		// Setup a payload chunk with only the columns we care about
		payload_chunk.Initialize(ctx, vector<LogicalType>(output_column_count, LogicalType::VARCHAR));
	}

	void OnStartElement(const char *name, const char **atts) {
		if (!in_row && Matches(name, "row")) {
			in_row = true;
			current_row = TryGetRowIdx(atts);

			// Do we care about this row?
			if (current_row >= start_row) {
				in_row_range = true;
			}

			return;
		}
		if (in_row_range && !in_col && Matches(name, "c")) {
			in_col = true;
			current_col = TryGetColumnIndex(atts);
			current_cell_type = CellTypes::FromAttributes(atts);

			// Do we care about this column?
			// try to get the column index from the cell ref
			if (current_col < col_count) {
				in_col_range = true;
			} else {
				in_col_range = false;
			}

			return;
		}
		if (in_col_range && !in_val && Matches(name, "v")) {
			in_val = true;
			EnableTextParsing(true);
			return;
		}
		if (in_col_range && !in_is && Matches(name, "is")) {
			in_is = true;
			return;
		}
		if (in_is && !in_t && Matches(name, "t")) {
			in_t = true;
			EnableTextParsing(true);
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (in_row && Matches(name, "row")) {
			in_row = false;

			if (in_row_range) {
				// Pad any missing columns with NULLs
				for (idx_t i = 0; i < col_count; i++) {
					if (!current_validity[i] && projection_map[i] != -1) {
						FlatVector::SetNull(payload_chunk.data[projection_map[i]], current_output_row, true);
					} else {
						// Reset the nullmask
						current_validity[i] = false;
					}
				}
				current_output_row++;
			}

			if (current_output_row == STANDARD_VECTOR_SIZE) {
				// Emit the chunk
				Suspend();
			}

			return;
		}
		if (in_row_range && in_col && Matches(name, "c")) {
			in_col = false;

			if (in_col_range && !current_cell_data.empty() && projection_map[current_col] != -1) {
				current_validity[current_col] = true;

				// Set the value in the payload chunk
				auto &payload_vec = payload_chunk.data[projection_map[current_col]];

				string_t blob;
				if (current_cell_type == CellType::SHARED_STRING) {
					auto idx = std::stoi(current_cell_data);
					// Since the string table is part of the global state, it will stay alive until the end of the
					// pipeline. We can just reference the string directly then without having to copy it to the vector
					blob = string_table.GetString(idx);
				} else {
					blob = StringVector::AddStringOrBlob(payload_vec, current_cell_data);
				}
				FlatVector::GetData<string_t>(payload_vec)[current_output_row] = blob;
			}
			current_cell_data.clear();
			return;
		}
		if (in_col_range && in_val && Matches(name, "v")) {
			in_val = false;
			EnableTextParsing(false);
			return;
		}
		if (in_col_range && in_is && Matches(name, "is")) {
			in_is = false;
			return;
		}
		if (in_is && in_t && Matches(name, "t")) {
			in_t = false;
			EnableTextParsing(false);
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_val || in_t) {
			current_cell_data.append(s, len);
		}
	}
};

struct XLSXReaderGlobalState : public GlobalTableFunctionState {
	static constexpr idx_t BUFFER_SIZE = 2048;

	shared_ptr<ZipArchiveFileHandle> archive;
	unique_ptr<DenseStringTable> string_table;
	unique_ptr<ZipArchiveExtractStream> sheet_stream;
	unique_ptr<XLSXSheetReader> sheet_reader;
	char buffer[BUFFER_SIZE];
	XMLParseResult status;

	// Projected column ids
	vector<idx_t> column_ids;

	// The number of empty rows to pad in the beginning
	idx_t initial_empty_rows = 0;

	explicit XLSXReaderGlobalState(ClientContext &context)
	    : string_table(make_uniq<DenseStringTable>(BufferAllocator::Get(context))) {
	}
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_info = input.bind_data->Cast<XLSXBindInfo>();
	auto result = make_uniq<XLSXReaderGlobalState>(context);

	result->column_ids = input.column_ids;

	auto &fs = FileSystem::GetFileSystem(context);
	result->archive = ZipArchiveFileHandle::Open(fs, bind_info.file_name);

	// Parse the string dictionary
	if (bind_info.string_dict_idx != -1) {
		auto string_dict_stream = result->archive->Extract(bind_info.string_dict_idx);
		XLSXDenseStringTableParser string_dict_parser(*result->string_table);
		string_dict_parser.ParseUntilEnd(string_dict_stream);
	}

	// Setup the projection map
	vector<idx_t> projection_map(bind_info.col_count, -1);
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		projection_map[input.column_ids[i]] = i;
	}

	// Setup the scanner
	result->sheet_reader = make_uniq<XLSXSheetReader>(context, *result->string_table, bind_info.start_row,
	                                                  projection_map, input.column_ids.size());

	result->status = XMLParseResult::OK;

	// Setup the stream
	result->sheet_stream = result->archive->Extract(bind_info.sheet_file_idx);
	result->initial_empty_rows = bind_info.initial_empty_rows;

	return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------

static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<XLSXReaderGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<XLSXBindInfo>();
	auto &reader = *state.sheet_reader;
	auto &stream = *state.sheet_stream;
	auto &status = state.status;

	if (state.initial_empty_rows != 0) {
		auto empty_output_rows = std::min<idx_t>(state.initial_empty_rows, STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < empty_output_rows; i++) {
			for (idx_t j = 0; j < output.ColumnCount(); j++) {
				FlatVector::SetNull(output.data[j], i, true);
			}
		}
		state.initial_empty_rows -= empty_output_rows;
		output.SetCardinality(empty_output_rows);
		return;
	}

	reader.current_output_row = 0;
	reader.payload_chunk.Reset();

	while (!stream.IsDone()) {
		if (status == XMLParseResult::SUSPENDED) {
			// We're suspended, we need to resume
			status = reader.Resume();
			if (status == XMLParseResult::SUSPENDED) {
				// Still suspended, emit the chunk
				break;
			}
		}
		// Otherwise, read more data
		auto read_size = stream.Read(state.buffer, sizeof(state.buffer));
		status = reader.Parse(state.buffer, read_size, stream.IsDone());
		if (status == XMLParseResult::SUSPENDED) {
			break;
		}
	}

	// Flush the current chunk
	output.SetCardinality(reader.current_output_row);
	if (reader.current_output_row != 0) {
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			/*
			// Special case for datetime bullshit
			auto &output_type = output.data[i].GetType();
			auto &cell_type = bind_data.cell_types[state.column_ids[i]];
			if(output_type == LogicalType::DATE && cell_type == CellType::NUMBER) {
			    // The cell is a number, and its the number of days since 1900-01-01
			    // We need to convert it to a date
			    auto input_vec = FlatVector::GetData<string_t>(reader.payload_chunk.data[i]);
			    auto output_vec = FlatVector::GetData<date_t>(output.data[i]);

			    Vector intermediate(LogicalType::INTEGER);
			    VectorOperations::DefaultCast(reader.payload_chunk.data[i], intermediate, reader.current_output_row);
			    auto int_vec = FlatVector::GetData<int32_t>(intermediate);

			    for(idx_t j = 0; j < reader.current_output_row; j++) {
			        if(FlatVector::Validity(intermediate).RowIsValid(j)) {
			            auto days = int_vec[j];
			            auto start = Date::FromDate(1900, 1, 1);
			            start.days += days;
			            output_vec[j] = start;
			        } else {
			            FlatVector::SetNull(output.data[i], j, true);
			        }
			    }
			} else {
			 */
			// Otherwise, just try to parse the string
			VectorOperations::DefaultCast(reader.payload_chunk.data[i], output.data[i], reader.current_output_row);
		}
	}
}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------
static unique_ptr<TableRef> ReplacementScan(ClientContext &, const string &table_name, ReplacementScanData *) {
	auto lower_name = StringUtil::Lower(table_name);
	if (StringUtil::EndsWith(lower_name, ".xlsx")) {
		auto table_function = make_uniq<TableFunctionRef>();
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
		table_function->function = make_uniq<FunctionExpression>("read_xlsx", std::move(children));
		return std::move(table_function);
	}
	// else not something we can replace
	return nullptr;
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void XLSXReader::Register(DatabaseInstance &db) {

	TableFunction xlsx_reader("read_xlsx", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal);
	xlsx_reader.named_parameters["sheet"] = LogicalType::VARCHAR;
	xlsx_reader.named_parameters["header"] = LogicalType::BOOLEAN;
	xlsx_reader.named_parameters["skip"] = LogicalType::INTEGER;
	xlsx_reader.named_parameters["all_varchar"] = LogicalType::BOOLEAN;

	xlsx_reader.projection_pushdown = true;

	ExtensionUtil::RegisterFunction(db, xlsx_reader);
	db.config.replacement_scans.emplace_back(ReplacementScan);
}

} // namespace duckdb
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

static inline bool matches(const char *a, const char *b) {
	return strcmp(a, b) == 0;
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
			return LogicalType::DATE;
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
		} else if (strcmp(str, "str") == 0) {
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
		for(int i = 0; atts[i]; i += 2) {
			if(strcmp(atts[i], "t") == 0) {
				return CellTypes::FromString(atts[i + 1]);
			}
		}
		return CellType::NUMBER;
	}
};

static string ColumnIdxToExcelName(idx_t col) {
	string result;
	while(col > 0) {
		auto remainder = col % 26;
		col /= 26;
		if(remainder == 0) {
			remainder = 26;
			col--;
		}
		result = (char)('A' + remainder - 1) + result;
	}
	return result;
}

static idx_t TryGetColumnIndex(const char **atts) {
    for(int i = 0; atts[i]; i += 2) {
        if(strcmp(atts[i], "r") == 0) {
            auto cell_ref = atts[i+1];
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
    for(int i = 0; atts[i]; i += 2) {
        if(strcmp(atts[i], "r") == 0) {
            idx_t value = std::stoi(atts[i+1]);
            return value - 1; // 0 indexed
        }
    }
    throw IOException("Failed to find column index, found no 'r' attribute");
}

//------------------------------------------------------------------------------
// XLSX Parsers
//------------------------------------------------------------------------------

class XLSXWorkBookParser : public XMLStateMachine<XLSXWorkBookParser> {
private:
    bool in_workbook = false;
    bool in_sheets = false;
    bool in_sheet = false;
public:

    vector<std::pair<string, idx_t>> sheets;

    void OnStartElement(const char *name, const char **atts) {
        if(!in_workbook && matches(name, "workbook")) {
            in_workbook = true;
            return;
        }
        if(in_workbook && !in_sheets && matches(name, "sheets")) {
            in_sheets = true;
            return;
        }
        if(in_workbook && in_sheets && !in_sheet && matches(name, "sheet")) {
            in_sheet = true;
            string sheet_name;
            bool found_id = false;
            idx_t sheet_id;
            for(int i = 0; atts[i]; i += 2) {
                if(strcmp(atts[i], "name") == 0) {
                    sheet_name = string(atts[i + 1]);
                }
                else if(strcmp(atts[i], "sheetId") == 0) {
                    sheet_id = atoi(atts[i + 1]);
                    found_id = true;
                }
            }

            if(found_id && !sheet_name.empty()) {
                sheets.emplace_back(sheet_name, sheet_id);
            }
        }
    }

    void OnEndElement(const char *name) {
        if(in_workbook && in_sheets && in_sheet && matches(name, "sheet")) {
            in_sheet = false;
            return;
        }
        if(in_workbook && in_sheets && matches(name, "sheets")) {
            in_sheets = false;
            return;
        }
        if(in_workbook && matches(name, "workbook")) {
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
    enum class State {
        START,
        ROW,
        COLUMN,
        VALUE
    } state = State::START;

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
    vector<CellType> header_types;

    // The data of the first row (after the header, if present)
    vector<CellType> row_types;
    vector<string> row_data;

    void OnStartElement(const char *name, const char **atts) {
        switch(state) {
            case State::START: {
                if(matches(name, "row")) {
                    state = State::ROW;
                    current_row = TryGetRowIdx(atts);

                    if(!found_row) {
                        found_row = true;
                        first_row = current_row;
                    }
                    row_has_any_data = false;
                }
            } break;
            case State::ROW: {
                if(matches(name, "c")) {
                    state = State::COLUMN;
                    current_col = TryGetColumnIndex(atts);
                    if(current_col >= end_column) {
                        end_column = current_col;
                        row_types.resize(current_col + 1, CellType::NUMBER);
                        row_data.resize(current_col + 1, "");
                    }
                    row_types[current_col] = CellTypes::FromAttributes(atts);
                }
            } break;
            case State::COLUMN: {
                if(matches(name, "v")) {
                    state = State::VALUE;
                    EnableTextParsing(true);
                }
            } break;
            default: break;
        }
    }

    void OnEndElement(const char *name) {
        switch(state) {
            case State::ROW: {
                if(matches(name, "row")) {
                    state = State::START;

                    // If we dont have a start row set, we set it to the first row that has any data
                    if(!has_start_row && row_has_any_data) {
                        has_start_row = true;
                        start_row = current_row;
                    }


                    if(start_row < first_row) {
                        // There is going to be empty padding rows, so there cant be a header
                        if(header_mode == XLSXHeaderMode::ALWAYS) {
                            // still, increment the start row
                            start_row++;
                        }
                        Stop();
                        return;
                    }

                    if(has_start_row && current_row >= start_row) {
                        // Alright, we found one row with data. Should we continue?
                        if(found_header || header_mode == XLSXHeaderMode::NEVER) {
                            // No, we're done. We either found a header or we only want the data
                            Stop();
                            return;
                        }
                        else if(header_mode == XLSXHeaderMode::ALWAYS) {
                            // Yes, we always read the first row as the header, so now we want the data
                            found_header = true;
                            header_data = row_data;
                            header_types = row_types;
                            start_row++;
                        }
                        else if(header_mode == XLSXHeaderMode::AUTO) {
                            // Maybe, try to automatically detect if the first row is a header
                            // We do this by checking if the first row is all strings (or empty)
                            // If it is, we treat it as a header and continue, otherwise we stop
                            bool all_strings = true;
                            for(idx_t i = 0; i < row_types.size(); i++) {
                                auto &type = row_types[i];
                                bool is_empty = row_data[i].empty();
                                if((type != CellType::STRING && type != CellType::SHARED_STRING) || is_empty) {
                                    all_strings = false;
                                    break;
                                }
                            }
                            if(all_strings) {
                                // All are strings! Assume this is a header and continue
                                found_header = true;
                                header_data = row_data;
                                header_types = row_types;
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
                    row_types.resize(end_column + 1, CellType::NUMBER);
                    return;
                }
            } break;
            case State::COLUMN: {
                if(matches(name, "c")) {
                    state = State::ROW;
                }
            } break;
            case State::VALUE: {
                if(matches(name, "v")) {
                    state = State::COLUMN;
                    EnableTextParsing(false);
                }
            } break;
            default: break;
        }
    }

    void OnCharacterData(const char *s, int len) {
        if(state == State::VALUE) {
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
        if(matches(name, "t")) {
            in_t_tag = true;
            EnableTextParsing(true);
            return;
        }
    }

    void OnEndElement(const char *name) {
        if(matches(name, "t")) {
            in_t_tag = false;
            EnableTextParsing(false);

            if(requested_string_indices.find(current_string_idx) != requested_string_indices.end()) {
                // We care about this string
                string_table.emplace(current_string_idx, current_string);
                requested_string_indices.erase(current_string_idx);
                if(requested_string_indices.empty()) {
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
        if(in_t_tag) {
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
        }
        else if (key == "header") {
            has_first_row_is_header = true;
            first_row_is_header = value.GetValue<bool>();
        }
        else if (key == "skip") {
            has_initial_rows_to_skip = true;
            initial_rows_to_skip = value.GetValue<int>();
        }
        else if (key == "all_varchar") {
            result->all_varchar = value.GetValue<bool>();
        }
        else {
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
    if(workbook_parser.sheets.empty()) {
        throw IOException("Failed to find any sheets in file '%s'", result->file_name);
    }

    // Look for the sheet, if none is specified, use first one
    idx_t sheet_id = 0;
    if (!result->sheet_name.empty()) {
        // Check if the sheet is in the workbook
        bool found_sheet = false;
        for(auto &sheet : workbook_parser.sheets) {
            if(sheet.first == result->sheet_name) {
                sheet_id = sheet.second;
                found_sheet = true;
                break;
            }
        }
        if(!found_sheet) {
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
    if(!archive->TryGetEntryIndexByName(target_path, result->sheet_file_idx)) {
        throw IOException("Failed to find sheet with id '%d'", sheet_id);
    }

    D_ASSERT(result->sheet_file_idx >= 0);
    D_ASSERT(!result->sheet_name.empty());

    // Look for the string dictionary in the archive
    if(!archive->TryGetEntryIndexByName("xl/sharedStrings.xml", result->string_dict_idx)) {
        throw IOException("Failed to find string dictionary in file '%s'", result->file_name);
    }

    // Now we can parse the sheet
    XLSXSheetSniffer sniffer;
    sniffer.has_start_row = has_initial_rows_to_skip;
    sniffer.start_row = initial_rows_to_skip;
    sniffer.header_mode = has_first_row_is_header
            ? (first_row_is_header
                ? XLSXHeaderMode::ALWAYS
                : XLSXHeaderMode::NEVER)
            : XLSXHeaderMode::AUTO;


    // Execute sniffer
    auto sheet_stream = archive->Extract(result->sheet_file_idx);
    sniffer.ParseUntilEnd(sheet_stream);

    if(sniffer.row_types.empty()) {
        throw IOException("Failed to find any data in sheet '%s'", result->sheet_name);
    }

    // Set based on the sniffer info
    result->first_row_is_header = sniffer.found_header;
    result->start_row = sniffer.start_row;
    result->col_count = sniffer.end_column + 1; // We start at col 0. If we end at 2, we have 3 columns
    result->initial_empty_rows = sniffer.start_row > sniffer.first_row ? 0 : (sniffer.first_row - sniffer.start_row);

    // Set the return types
    for (idx_t i = 0; i < sniffer.row_types.size(); i++) {
        if (result->all_varchar) {
            return_types.push_back(LogicalType::VARCHAR);
        } else {
            // Special case: treat empty rows as VARCHAR instead of defaulting to DOUBLE
            if (sniffer.row_data[i].empty()) {
                return_types.push_back(LogicalType::VARCHAR);
            } else {
                return_types.push_back(CellTypes::ToLogicalType(sniffer.row_types[i]));
            }
        }
    }

    // Now set the names.
    if(!result->first_row_is_header) {
        // No header, just give them the standard excel names
        for(idx_t i = 0; i < sniffer.row_types.size(); i++) {
            names.push_back(ColumnIdxToExcelName(i + 1)); // 1 indexed
        }
    } else {
        // If any names contain shared strings, we do a sparse scan of the string dictionary to find them.
        // Is there any shared strings in the header?
        vector<idx_t> shared_string_indices;
        for(idx_t i = 0; i < sniffer.header_types.size(); i++) {
            if(sniffer.header_types[i] == CellType::SHARED_STRING) {
                shared_string_indices.push_back(std::stoi(sniffer.header_data[i]));
            }
        }
        if(shared_string_indices.empty()) {
            // No shared strings, all the names are already in the header
            names = sniffer.header_data;
        } else {
            // Scan the string dictionary for the shared strings
            auto string_dict_stream = archive->Extract(result->string_dict_idx);
            XLSXSparseStringTableParser parser;
            parser.requested_string_indices.insert(shared_string_indices.begin(), shared_string_indices.end());
            parser.ParseUntilEnd(string_dict_stream);

            // Now we can set the names
            for(idx_t i = 0; i < sniffer.header_types.size(); i++) {
                if (sniffer.header_types[i] == CellType::SHARED_STRING) {
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
        for(idx_t i = 0; i < names.size(); i++) {
            if(names[i].empty()) {
                names[i] = "col" + std::to_string(i);
            }
            for(idx_t j = 0; j < i; j++) {
                if(names[i] == names[j]) {
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
    if(types_size < names_size) {
        // We have more names than types, so we need to add types
        for(auto i = types_size; i < names_size; i++) {
            return_types.push_back(LogicalType::VARCHAR);
        }
    }

    return std::move(result);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

class XLSXDenseStringTableParser : public XMLStateMachine<XLSXDenseStringTableParser> {
    bool in_t_tag = false;
    idx_t current_string_idx = 0;
    string current_string;
    vector<string> &string_table;
public:
    explicit XLSXDenseStringTableParser(vector<string> &string_table_p)
            : string_table(string_table_p) { }

    void OnStartElement(const char *name, const char **atts) {
        // TODO: Harden this to only match the t tag in the right namespace
        if(strcmp(name, "t") == 0) {
            in_t_tag = true;
            EnableTextParsing(true);
            return;
        }

        if(strcmp(name, "sst") == 0) {
            // Optimization: look for the "uniqueCount" attribute value
            // and reserve space for that many strings
            for (int i = 0; atts[i]; i += 2) {
                if ((strcmp(atts[i], "uniqueCount") == 0)) {
                    // Reserve space for the strings
                    auto count = atoi(atts[i + 1]);
                    string_table.reserve(count);
                    return;
                }
            }
            return;
        }
    }

    void OnEndElement(const char *name) {
        if(strcmp(name, "t") == 0) {
            in_t_tag = false;
            EnableTextParsing(false);
            string_table.push_back(current_string);
            current_string.clear();
            current_string_idx++;
            return;
        }
    }

    void OnCharacterData(const char *s, int len) {
        if(in_t_tag) {
            current_string.append(s, len);
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

    const vector<string> &string_table;

    idx_t start_row = 0;
    idx_t current_row = 0;
    idx_t current_col = 0;
    idx_t col_count = 0;

    CellType current_cell_type;
    string current_cell_data;
    vector<bool> current_validity;
public:
    idx_t current_output_row = 0;
    DataChunk payload_chunk;

    explicit XLSXSheetReader(ClientContext &ctx, const vector<string> &string_table_p, idx_t start_row_p, idx_t col_count_p)
    : string_table(string_table_p), start_row(start_row_p), col_count(col_count_p), current_validity(col_count_p, false) {

        // Setup a payload chunk with only the columns we care about
        payload_chunk.Initialize(ctx, vector<LogicalType>(col_count, LogicalType::VARCHAR));
    }

    void OnStartElement(const char *name, const char **atts) {
        if(!in_row && matches(name, "row")) {
            in_row = true;
            current_row = TryGetRowIdx(atts);

            // Do we care about this row?
            if(current_row >= start_row) {
                in_row_range = true;
            }

            return;
        }
        if(in_row_range && !in_col && matches(name, "c")) {
            in_col = true;
            current_col = TryGetColumnIndex(atts);
            current_cell_type = CellTypes::FromAttributes(atts);

            // Do we care about this column?
            // try to get the column index from the cell ref
            if(current_col < col_count) {
                in_col_range = true;
            } else {
                in_col_range = false;
            }

            return;
        }
        if(in_col_range && !in_val && matches(name, "v")) {
            in_val = true;
            EnableTextParsing(true);
            return;
        }
    }

    void OnEndElement(const char *name) {
        if(in_row && matches(name, "row")) {
            in_row = false;

            if (in_row_range) {
                // Pad any missing columns with NULLs
                for (idx_t i = 0; i < col_count; i++) {
                    if (!current_validity[i]) {
                        FlatVector::SetNull(payload_chunk.data[i], current_output_row, true);
                    } else {
                        // Reset the nullmask
                        current_validity[i] = false;
                    }
                }
                current_output_row++;
            }

            if(current_output_row == STANDARD_VECTOR_SIZE) {
                // Emit the chunk
                Suspend();
            }

            return;
        }
        if(in_row_range && in_col && matches(name, "c")) {
            in_col = false;

            if(in_col_range && !current_cell_data.empty()) {
                current_validity[current_col] = true;

                // Set the value in the payload chunk
                auto &payload_vec = payload_chunk.data[current_col];

                string_t blob;
                if (current_cell_type == CellType::SHARED_STRING) {
                    auto idx = std::stoi(current_cell_data);
                    if (idx >= string_table.size()) {
                        throw IOException("Failed to find shared string with index %d", idx);
                    }
                    auto entry = string_table[idx];
                    blob = StringVector::AddStringOrBlob(payload_vec, entry);
                } else {
                    blob = StringVector::AddStringOrBlob(payload_vec, current_cell_data);
                }
                FlatVector::GetData<string_t>(payload_vec)[current_output_row] = blob;
            }
            current_cell_data.clear();
            return;
        }
        if(in_col_range && in_val && matches(name, "v")) {
            in_val = false;
            EnableTextParsing(false);
        }
    }

    void OnCharacterData(const char *s, int len) {
        if(in_val) {
            current_cell_data.append(s, len);
        }
    }
};


struct XLSXReaderGlobalState : public GlobalTableFunctionState {
    shared_ptr<ZipArchiveFileHandle> archive;
    unique_ptr<vector<string>> string_table;
    unique_ptr<ZipArchiveExtractStream> sheet_stream;
    unique_ptr<XLSXSheetReader> sheet_reader;
    char buffer[2048];
    XMLParseResult status;

    // Projected column ids
    vector<idx_t> column_ids;
    // Reverse projection, maps from column id to projected column id
    vector<idx_t> reverse_projection;

    // The number of empty rows to pad in the beginning
    idx_t initial_empty_rows = 0;
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_info = input.bind_data->Cast<XLSXBindInfo>();
	auto result = make_uniq<XLSXReaderGlobalState>();

    result->column_ids = input.column_ids;
    result->reverse_projection.resize(bind_info.col_count, -1);
    for(idx_t i = 0; i < input.column_ids.size(); i++) {
        result->reverse_projection[input.column_ids[i]] = i;
    }

    auto &fs = FileSystem::GetFileSystem(context);
    result->archive = ZipArchiveFileHandle::Open(fs, bind_info.file_name);

    // Parse the string dictionary
    result->string_table = make_uniq<vector<string>>();
    auto string_dict_stream = result->archive->Extract(bind_info.string_dict_idx);
    XLSXDenseStringTableParser string_dict_parser(*result->string_table);
    string_dict_parser.ParseUntilEnd(string_dict_stream);

    // Setup the scanner
    result->sheet_reader = make_uniq<XLSXSheetReader>(context, *result->string_table, bind_info.start_row, bind_info.col_count);

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
    auto &reader = *state.sheet_reader;
    auto &stream = *state.sheet_stream;
    auto &status = state.status;

    if(state.initial_empty_rows != 0) {
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

    while(!stream.IsDone()) {
        if(status == XMLParseResult::SUSPENDED) {
            // We're suspended, we need to resume
            status = reader.Resume();
            if(status == XMLParseResult::SUSPENDED) {
                // Still suspended, emit the chunk
                break;
            }
        }
        // Otherwise, read more data
        auto read_size = stream.Read(state.buffer, sizeof(state.buffer));
        status = reader.Parse(state.buffer, read_size, stream.IsDone());
        if(status == XMLParseResult::SUSPENDED) {
            break;
        }
    }

    // Flush the current chunk
    output.SetCardinality(reader.current_output_row);
    if(reader.current_output_row != 0) {
        for (idx_t i = 0; i < output.ColumnCount(); i++) {
            VectorOperations::DefaultCast(reader.payload_chunk.data[state.column_ids[i]], output.data[i],
                                          reader.current_output_row);
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


}
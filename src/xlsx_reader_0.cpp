#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/typedefs.hpp"
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
		for (int i = 0; atts[i]; i += 2) {
			if (strcmp(atts[i], "t") == 0) {
				return CellTypes::FromString(atts[i + 1]);
			}
		}
		return CellType::NUMBER;
	}
};

static string ToExcelColumnName(idx_t col) {
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

struct XLSXWorkBookParser : public XMLStateMachine<XLSXWorkBookParser> {
	bool in_workbook = false;
	bool in_sheets = false;
	bool in_sheet = false;
	vector<std::pair<string, idx_t>> sheets;

	void OnStartElement(const char *name, const char **atts) {
		if (!in_workbook && matches(name, "workbook")) {
			in_workbook = true;
			return;
		}
		if (in_workbook && !in_sheets && matches(name, "sheets")) {
			in_sheets = true;
			return;
		}
		if (in_workbook && in_sheets && !in_sheet && matches(name, "sheet")) {
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
		if (in_workbook && in_sheets && in_sheet && matches(name, "sheet")) {
			in_sheet = false;
			return;
		}
		if (in_workbook && in_sheets && matches(name, "sheets")) {
			in_sheets = false;
			return;
		}
		if (in_workbook && matches(name, "workbook")) {
			in_workbook = false;
			Stop();
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
	}
};

enum class HeaderMode { ALWAYS, NEVER, AUTO };

struct XLSXSheetSniffer : public XMLStateMachine<XLSXSheetSniffer> {

	bool has_start_row = true;
	idx_t start_row = 0;

	idx_t current_row = 0;
	idx_t current_column = 0;
	idx_t total_column_count = 0;

	bool in_row = false;
	bool in_column = false;
	bool in_value = false;

	vector<string> row_data;
	vector<CellType> row_types;
	bool row_has_any_data = false;

	HeaderMode header_mode = HeaderMode::AUTO;
	bool found_header = false;
	vector<string> header_data;
	vector<CellType> header_types;

	void OnStartElement(const char *name, const char **atts) {
		if (!in_row && matches(name, "row")) {
			in_row = true;
			row_has_any_data = false;
			return;
		}
		if (in_row && !in_column && matches(name, "c")) {
			in_column = true;
			row_types.emplace_back(CellTypes::FromAttributes(atts));
			row_data.emplace_back();
			return;
		}
		if (in_row && in_column && !in_value && matches(name, "v")) {
			in_value = true;
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (in_row && matches(name, "row")) {
			in_row = false;
			total_column_count = std::max(total_column_count, current_column);

			// if we dont have a start row set, we set it to the first row that has any data
			if (!has_start_row && row_has_any_data) {
				has_start_row = true;
				start_row = current_row;
			}

			if (has_start_row && current_row >= start_row) {
				// Should we continue?
				if (found_header || header_mode == HeaderMode::NEVER) {
					// No, we're done. We either found a header or we only want the data
					Stop();
					return;
				} else if (header_mode == HeaderMode::ALWAYS) {
					// Yes, we always treat the first row as a header, so if we haven't found it yet, this is it
					found_header = true;
					header_data = row_data;
					header_types = row_types;
					start_row++;
				} else if (header_mode == HeaderMode::AUTO) {
					// Maybe, we automatically detect if the first row is a header
					// We do this by checking if the first row is all strings
					// If it is, we treat it as a header and continue, otherwise stop
					bool all_strings = true;
					for (const auto &type : row_types) {
						if (type != CellType::SHARED_STRING && type != CellType::STRING) {
							all_strings = false;
							break;
						}
					}
					if (all_strings) {
						// All are strings! Treat this row as a header and continue
						found_header = true;
						header_data = row_data;
						header_types = row_types;
						start_row++;
					} else {
						found_header = false;
						Stop();
						return;
					}
				}
			}

			row_data.clear();
			row_types.clear();

			current_row++;
			current_column = 0;
			return;
		}
		if (in_row && in_column && matches(name, "c")) {
			in_column = false;
			current_column++;
			return;
		}
		if (in_row && in_column && in_value && matches(name, "v")) {
			in_value = false;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_value) {
			row_data.back().append(s, len);
			row_has_any_data = true;
		}
	}
};

class SparseStringTableParser : public XMLStateMachine<SparseStringTableParser> {
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
		if (matches(name, "t")) {
			in_t_tag = true;
			return;
		}
	}
	void OnEndElement(const char *name) {
		if (matches(name, "t")) {
			in_t_tag = false;
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
	// The index of the sheet in the archive
	int sheet_file_idx = -1;
	// The index of the string dictionary in the archive
	int string_dict_idx = -1;
	// Whether or not to use the first row as the header
	bool first_row_is_header = false;
	// Skip empty rows
	bool skip_empty_rows = false;
	// Skip n rows
	int initial_rows_to_skip = 0;
	// Whether or not all columns should be treated as VARCHAR
	bool all_varchar = false;

	// The number of columns to output
	idx_t column_count = 0;

	// The total number of columns in the sheet
	idx_t total_column_count = 0;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XLSXBindInfo>();

	// first parameter is the file name
	result->file_name = input.inputs[0].GetValue<string>();

	bool has_initial_rows_to_skip = false;
	idx_t initial_rows_to_skip = 0;
	bool has_first_row_is_header = false;
	bool first_row_is_header = false;

	for (auto &kv : input.named_parameters) {
		auto &name = kv.first;
		auto &value = kv.second;
		if (name == "sheet") {
			result->sheet_name = value.GetValue<string>();
		} else if (name == "header") {
			has_first_row_is_header = true;
			first_row_is_header = value.GetValue<bool>();
		} else if (name == "skip") {
			has_initial_rows_to_skip = true;
			initial_rows_to_skip = value.GetValue<int>();
		} else if (name == "skip_empty") {
			result->skip_empty_rows = value.GetValue<bool>();
		} else if (name == "all_varchar") {
			result->all_varchar = value.GetValue<bool>();
		}
	}

	// Open the file
	auto &fs = FileSystem::GetFileSystem(context);
	auto archive = ZipArchiveFileHandle::Open(fs, result->file_name);

	// Parse the workbook
	XLSXWorkBookParser workbook_parser;
	auto workbook_stream = archive->Extract("xl/workbook.xml");
	workbook_parser.ParseUntilEnd(workbook_stream);

	// Are there any sheets in the workbook?
	if (workbook_parser.sheets.empty()) {
		throw IOException("Failed to find any sheets in file '%s'", result->file_name);
	}

	// Look for the sheet, if none is specified, use the first one
	idx_t sheet_id = 0;
	if (result->sheet_name.empty()) {
		// Pick the first sheet available
		auto first_sheet = *workbook_parser.sheets.begin();
		result->sheet_name = first_sheet.first;

		sheet_id = first_sheet.second;
	} else {
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
	}

	// The "sheet id" is not the same as the file index in the archive, so we have to look it up here
	// Look for the requested sheet in the archive
	auto target_path = StringUtil::Format("xl/worksheets/sheet%d.xml", sheet_id);
	// If this succeeds, we set the result sheet_file_idx
	if (!archive->TryGetEntryIndexByName(target_path, result->sheet_file_idx)) {
		throw IOException("Failed to find sheet with id '%s' in file \n%s", sheet_id);
	}

	D_ASSERT(result->sheet_file_idx >= 0);
	D_ASSERT(!result->sheet_name.empty());

	// Look for the string dictionary in the archive
	if (!archive->TryGetEntryIndexByName("xl/sharedStrings.xml", result->string_dict_idx)) {
		throw IOException("Failed to find string dictionary in file '%s'", result->file_name);
	}

	// Parse the sheet
	XLSXSheetSniffer sniffer;
	sniffer.header_mode =
	    has_first_row_is_header ? (first_row_is_header ? HeaderMode::ALWAYS : HeaderMode::NEVER) : HeaderMode::AUTO;
	sniffer.has_start_row = has_initial_rows_to_skip;
	sniffer.start_row = initial_rows_to_skip;

	// Execute sniffer
	auto sheet_stream = archive->Extract(result->sheet_file_idx);
	sniffer.ParseUntilEnd(sheet_stream);

	if (sniffer.row_types.empty()) {
		throw IOException("Failed to find any data in sheet '%s'", result->sheet_name);
	}

	// Set based on the sniffer info
	result->first_row_is_header = sniffer.found_header;
	result->initial_rows_to_skip = sniffer.start_row;
	result->total_column_count = sniffer.total_column_count;
	result->column_count = sniffer.total_column_count;

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

	// Now set the names. We may have to scan the shared string table for this if any name contains shared_strings
	if (result->first_row_is_header) {
		// Is there any shared string in the header?
		vector<idx_t> shared_string_indices;
		for (idx_t i = 0; i < sniffer.header_types.size(); i++) {
			if (sniffer.header_types[i] == CellType::SHARED_STRING) {
				shared_string_indices.push_back(std::stoi(sniffer.header_data[i]));
			}
		}
		if (shared_string_indices.empty()) {
			names = sniffer.header_data;
		} else {
			// We have to parse the shared string table
			SparseStringTableParser parser;
			parser.requested_string_indices.insert(shared_string_indices.begin(), shared_string_indices.end());
			auto string_dict_stream = archive->Extract(result->string_dict_idx);
			parser.ParseUntilEnd(string_dict_stream);

			for (idx_t i = 0; i < sniffer.header_types.size(); i++) {
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
	} else {
		// Just give them standard names.
		for (idx_t i = 0; i < sniffer.row_types.size(); i++) {
			names.push_back(ToExcelColumnName(i + 1)); // 1 indexed
		}
	}

	auto types_size = return_types.size();
	auto names_size = names.size();
	// If the header was larger than the data, fill missing column with varchar
	if (types_size < names_size) {
		for (idx_t i = types_size; i < names_size; i++) {
			return_types.push_back(LogicalType::VARCHAR);
		}
	}
	// If the data was larger than the header, fill missing names with default names
	if (names_size < types_size) {
		for (idx_t i = names_size; i < types_size; i++) {
			names.push_back(ToExcelColumnName(i + 1)); // 1 indexed
		}
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

class DenseStringTableParser : public XMLStateMachine<DenseStringTableParser> {
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	string current_string;
	vector<string> &string_table;

public:
	explicit DenseStringTableParser(vector<string> &string_table_p) : string_table(string_table_p) {
	}

	void OnStartElement(const char *name, const char **atts) {
		// TODO: Harden this to only match the t tag in the right namespace
		if (strcmp(name, "t") == 0) {
			in_t_tag = true;
			return;
		}

		if (strcmp(name, "sst") == 0) {
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
		if (strcmp(name, "t") == 0) {
			in_t_tag = false;
			string_table.push_back(current_string);
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

struct XLSSheetReader : public XMLStateMachine<XLSSheetReader> {
	idx_t start_row = 0;
	idx_t current_row = 0;
	idx_t current_column = 0;
	idx_t end_column = 0;
	idx_t current_output_row = 0;

	CellType current_cell_type;
	string current_data;
	string current_cell_ref;

	bool in_row = false;
	bool in_row_range = false;
	bool in_column = false;
	bool in_column_range = false;
	bool in_value = false;

	vector<string> &string_table;

public:
	DataChunk payload_chunk;

	explicit XLSSheetReader(ClientContext &ctx, vector<string> &string_table_p, idx_t column_count_p)
	    : string_table(string_table_p) {
		payload_chunk.Initialize(ctx, vector<LogicalType>(column_count_p, LogicalType::VARCHAR));
	}

	void OnStartElement(const char *name, const char **atts) {
		if (!in_row && strcmp(name, "row") == 0) {
			in_row = true;

			if (current_row >= start_row) {
				in_row_range = true;
			}
			return;
		}
		if (in_row_range && !in_column && strcmp(name, "c") == 0) {
			in_column = true;
			current_cell_type = CellTypes::FromAttributes(atts);

			if (current_column < end_column) {
				in_column_range = true;
			} else {
				in_column_range = false;
			}

			for (int i = 0; atts[i]; i += 2) {
				if (strcmp(atts[i], "r") == 0) {
					current_cell_ref = string(atts[i + 1]);
				}
			}

			return;
		}
		if (in_row_range && in_column_range && !in_value && strcmp(name, "v") == 0) {
			in_value = true;
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (in_row && strcmp(name, "row") == 0) {
			in_row = false;
			current_row++;

			if (in_row_range) {

				if (current_column < end_column) {
					// Pad the rest of the row with NULLs
					for (idx_t i = current_column; i < end_column; i++) {
						FlatVector::SetNull(payload_chunk.data[i], current_output_row, true);
					}
				}

				current_output_row++;
			}

			if (current_output_row == STANDARD_VECTOR_SIZE) {
				// Emit the chunk
				Suspend();
			}

			current_column = 0;
			return;
		}
		if (in_row_range && in_column && strcmp(name, "c") == 0) {
			in_column = false;

			if (in_column_range) {
				// Set the value in the payload chunk
				auto &payload_vec = payload_chunk.data[current_column];

				// First of all, we should check if the current cell has the ref we expect it to have.
				// If it doesn't, we need to fill in the missing cells with NULLs.
				auto expected_ref = ToExcelColumnName(current_column + 1) + std::to_string(current_row + 1);
				if (current_cell_ref != expected_ref) {
					// We need to fill in the missing cells with NULLs
					for (idx_t i = current_column; i < end_column; i++) {
						FlatVector::SetNull(payload_chunk.data[i], current_output_row, true);
					}
				}
				// Then check if the current cell is empty
				else if (current_data.empty()) {
					FlatVector::SetNull(payload_vec, current_output_row, true);
				} else {
					string_t blob;
					if (current_cell_type == CellType::SHARED_STRING) {
						auto idx = std::stoi(current_data);
						if (idx >= string_table.size()) {
							throw IOException("Failed to find shared string with index %d", idx);
						}
						auto entry = string_table[idx];
						blob = StringVector::AddStringOrBlob(payload_vec, entry);
					} else {
						blob = StringVector::AddStringOrBlob(payload_vec, current_data);
					}
					FlatVector::GetData<string_t>(payload_vec)[current_output_row] = blob;
					current_data.clear();
				}
			}

			current_cell_ref.clear();

			current_column++;
			return;
		}
		if (in_row_range && in_column_range && in_value && strcmp(name, "v") == 0) {
			in_value = false;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_value) {
			current_data.append(s, len);
		}
	}
};

struct XLSXReaderGlobalState : public GlobalTableFunctionState {
	shared_ptr<ZipArchiveFileHandle> archive;
	unique_ptr<vector<string>> string_table;
	unique_ptr<XLSSheetReader> sheet_reader;
	unique_ptr<ZipArchiveExtractStream> sheet_stream;
	char buffer[2048];
	XMLParseResult status;
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_info = input.bind_data->Cast<XLSXBindInfo>();
	auto result = make_uniq<XLSXReaderGlobalState>();

	auto &fs = FileSystem::GetFileSystem(context);
	result->archive = ZipArchiveFileHandle::Open(fs, bind_info.file_name);

	// Parse the string table
	result->string_table = make_uniq<vector<string>>();
	DenseStringTableParser parser(*result->string_table);
	auto string_table_stream = result->archive->Extract(bind_info.string_dict_idx);
	parser.ParseUntilEnd(string_table_stream);

	// Initialize the sheet reader
	result->sheet_reader = make_uniq<XLSSheetReader>(context, *result->string_table, bind_info.column_count);
	result->sheet_reader->start_row = bind_info.initial_rows_to_skip;
	result->sheet_reader->end_column = bind_info.column_count;

	// Setup stream
	result->sheet_stream = result->archive->Extract(bind_info.sheet_file_idx);

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
	xlsx_reader.named_parameters["skip_empty"] = LogicalType::BOOLEAN;
	xlsx_reader.named_parameters["all_varchar"] = LogicalType::BOOLEAN;

	ExtensionUtil::RegisterFunction(db, xlsx_reader);
	db.config.replacement_scans.emplace_back(ReplacementScan);
}

} // namespace duckdb
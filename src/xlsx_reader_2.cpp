#include "xlsx_reader.hpp"

#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "expat.h"
#include "miniz.hpp"
#include "xlsx_cell.hpp"
#include "xml_state_machine.hpp"

using namespace duckdb_miniz;

namespace duckdb {

struct MzZipArchive {
	unique_ptr<FileHandle> handle;
	mz_zip_archive archive;

	static size_t ReadCallback(void *user_data, mz_uint64 file_offset, void *buffer, size_t n) {
		auto handle = static_cast<FileHandle *>(user_data);
		handle->Seek(file_offset);
		return handle->Read(static_cast<data_ptr_t>(buffer), n);
	}

	explicit MzZipArchive(unique_ptr<FileHandle> handle_p) : handle(std::move(handle_p)) {
		memset(&archive, 0, sizeof(archive));
		archive.m_pRead = ReadCallback;
		archive.m_pIO_opaque = handle.get();

		auto ok = mz_zip_reader_init(&archive, handle->GetFileSize(), MZ_ZIP_FLAG_COMPRESSED_DATA); // TODO: Pass flags?
		if (!ok) {
			auto error = mz_zip_get_last_error(&archive);
			auto error_str = mz_zip_get_error_string(error);
			throw IOException("Failed to initialize zip reader: %s", error_str);
		}
	}

	~MzZipArchive() {
		mz_zip_reader_end(&archive);
	}
};

template <class T>
static void StreamToXMLMachine(XMLStateMachine<T> &machine, mz_zip_archive &archive, int sheet_idx) {
	// Stream the file into the parser, chunk by chunk
	mz_zip_reader_extract_to_callback(
	    &archive, sheet_idx,
	    [](void *user_data, mz_uint64 file_offset, const void *input_buffer, size_t n) {
		    auto &parser = *static_cast<XMLStateMachine<T> *>(user_data);
		    auto status = parser.Parse(static_cast<const char *>(input_buffer), n, false);
		    if (status == XMLParseResult::ABORTED) {
			    // Stop the extraction
			    return static_cast<size_t>(n);
		    } else if (status != XMLParseResult::OK && status != XMLParseResult::DONE) {
			    // We dont support suspending parsers here
			    throw InvalidInputException("Unexpected XML parse result: %d", status);
		    }
		    return n;
	    },
	    &machine, 0);

	machine.Parse(nullptr, 0, true);
}

//-----------------------------------------------------------------------------
// Spare String Table Parser
//-----------------------------------------------------------------------------
// Fetches the strings from the shared strings table, only copying and
// and allocating the strings that are requested
class SparseStringTableParser : public XMLStateMachine<SparseStringTableParser> {
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	vector<char> char_buffer;

public:
	// The indices of the strings we want to fetch
	unordered_set<idx_t> requested_string_indices;

	// The resulting strings we fetched
	unordered_map<idx_t, string> string_table;

	void OnStartElement(const char *name, const char **atts) {
		if (strcmp(name, "t") == 0) {
			in_t_tag = true;
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (strcmp(name, "t") == 0) {
			in_t_tag = false;
			if (requested_string_indices.find(current_string_idx) != requested_string_indices.end()) {
				// We care about this string
				string_table.emplace(current_string_idx, string(char_buffer.data(), char_buffer.size()));
				requested_string_indices.erase(current_string_idx);
				if (requested_string_indices.empty()) {
					// We're done
					Stop();
				}
			}

			char_buffer.clear();
			current_string_idx++;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_t_tag) {
			char_buffer.insert(char_buffer.end(), s, s + len);
		}
	}
};

//-----------------------------------------------------------------------------
// Dense String Table Parser
//-----------------------------------------------------------------------------
// Fetches all the strings from the shared strings table, copying and
// allocating all of them.
class DenseStringTableParser : public XMLStateMachine<DenseStringTableParser> {
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	vector<char> char_buffer;

public:
	vector<string> string_table;

	void OnStartElement(const char *name, const char **atts) {
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
			string_table.emplace_back(char_buffer.data(), char_buffer.size());
			char_buffer.clear();
			current_string_idx++;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_t_tag) {
			char_buffer.insert(char_buffer.end(), s, s + len);
		}
	}
};

//-----------------------------------------------------------------------------
// Sniffer
//-----------------------------------------------------------------------------
enum class HeaderMode {
	ALWAYS, // Always treat the first row as a header
	NEVER,  // Never treat the first row as a header
	AUTO,   // Automatically detect if the first row is a header
};

static CellType GetCellType(const char **atts) {
	for (int i = 0; atts[i]; i += 2) {
		if (strcmp(atts[i], "t") == 0) {
			return CellTypes::FromString(atts[i + 1]);
		}
	}
	return CellType::NUMBER;
}

class XLSXSniffer : public XMLStateMachine<XLSXSniffer> {
public:
	HeaderMode header_mode;

	// State
	vector<int> column_shared_string_indices;
	bool in_row = false;
	bool in_column = false;
	bool in_value = false;
	idx_t current_row = 0;
	idx_t current_column = 0;

	vector<char> parsed_column_data;
	CellType parsed_column_type;

	// Either detecting or set by the user
	bool has_start_row = false;
	bool has_start_column = false;
	bool has_end_column = false;
	idx_t start_row = 0;
	idx_t start_column = 0;
	idx_t end_column = 0;

	idx_t last_column_index = 0;

	vector<string> column_text;
	vector<CellType> column_types;

	bool found_header = false;
	vector<string> header_text;
	vector<CellType> header_types;

	explicit XLSXSniffer(HeaderMode header_mode_p) : header_mode(header_mode_p) {
	}

	bool IsInRange() {
		return has_start_row && has_start_column && current_row >= start_row && current_column >= start_column &&
		       (!has_end_column || current_column < end_column);
	}

	void OnStartElement(const char *name, const char **atts) {
		if (strcmp(name, "row") == 0) {
			in_row = true;
			return;
		}
		if (in_row && strcmp(name, "c") == 0) {
			in_column = true;
			parsed_column_type = GetCellType(atts);
			return;
		}
		if (in_column && strcmp(name, "v") == 0) {
			in_value = true;
			return;
		}
	}

	void OnColumnEnd() {
		in_column = false;

		if (!has_start_row && !parsed_column_data.empty()) {
			// We found the first row with data
			has_start_row = true;
			start_row = current_row;
		}

		if (!has_start_column && !parsed_column_data.empty()) {
			// We found the first column with data
			has_start_column = true;
			start_column = current_column;
		}

		if (has_start_column && !has_end_column && parsed_column_data.empty()) {
			// We found the last column with data
			has_end_column = true;
			end_column = current_column;
		}

		if (IsInRange()) {
			column_text.emplace_back(parsed_column_data.data(), parsed_column_data.size());
			column_types.emplace_back(parsed_column_type);
		}

		parsed_column_data.clear();
		current_column++;
	}

	void OnRowEnd() {
		in_row = false;
		current_row++;
		last_column_index = std::max(last_column_index, current_column);

		// Corner case: if we dont have an end column yet, but we have a row end
		// surely this is the last column
		if (has_start_column && !has_end_column) {
			has_end_column = true;
			end_column = current_column;
		}

		current_column = 0;

		// Are we in range?
		if (has_start_row && current_row > start_row) {
			// Should we continue?

			if (found_header || header_mode == HeaderMode::NEVER) {
				// No, we're done. We either found a header or we only want the data
				Stop();
				return;
			}

			if (header_mode == HeaderMode::ALWAYS) {
				// Yes, we always treat the first row as a header.
				// So if we haven't found one yet we treat this row as a header and continue
				found_header = true;
				header_text = column_text;
				header_types = column_types;
				column_text.clear();
				column_types.clear();
				return;
			}
			if (header_mode == HeaderMode::AUTO) {
				// Maybe, we automatically detect if the first row is a header
				// We do this by checking if the first row is all strings
				// If it is, we treat it as a header and continue, otherwise stop
				bool all_strings = true;
				for (auto &type : column_types) {
					if (type != CellType::SHARED_STRING && type != CellType::STRING) {
						all_strings = false;
						break;
					}
				}
				if (all_strings) {
					found_header = true;
					header_text = column_text;
					header_types = column_types;
					column_text.clear();
					column_types.clear();
				} else {
					found_header = false;
					Stop();
				}
			}
		}
		return;
	}

	void OnEndElement(const char *name) {
		if (strcmp(name, "row") == 0) {
			OnRowEnd();
			return;
		}

		if (in_row && strcmp(name, "c") == 0) {
			OnColumnEnd();
			return;
		}

		if (in_column && strcmp(name, "v") == 0) {
			in_value = false;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_value) {
			parsed_column_data.insert(parsed_column_data.end(), s, s + len);
		}
	}
};

//-----------------------------------------------------------------------------
// Bind
//-----------------------------------------------------------------------------

struct XLSXReaderBindData : public TableFunctionData {
	// The name of the file to read
	string file_name;
	// The name of the sheet to read
	string sheet_name;
	// The index of the sheet file in the zip archive
	int sheet_file_idx;
	// The index of the shared string file in the zip archive
	int shared_string_file_idx;

	// If the first row should be treated as a header
	bool first_row_is_header;

	// The range of cells to read
	idx_t range_row_start = 0;
	idx_t range_column_start = 0;
	idx_t range_column_end = 0;

	// If zero, read all rows
	idx_t range_row_end = 0;

	bool abort_on_empty_row = false;

	// The column types
	vector<LogicalType> column_types;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<XLSXReaderBindData>();
	// Get the file name

	result->file_name = input.inputs[0].GetValue<string>();

	// Get the sheet name (if any), otherwise use the first sheet
	for (auto &kv : input.named_parameters) {
		if (kv.first == "sheet") {
			result->sheet_name = kv.second.GetValue<string>();
			break;
		}
	}

	auto &fs = FileSystem::GetFileSystem(context);
	MzZipArchive archive(fs.OpenFile(result->file_name, FileFlags::FILE_FLAGS_READ));

	// Now get the sheet dimensions and columns
	// Get the index of the sheet
	result->sheet_file_idx = -1;
	if (result->sheet_name.empty()) {
		// No sheet name specified, loop over the files and find the first sheet
		for (auto i = 0; i < mz_zip_reader_get_num_files(&archive.archive); i++) {
			mz_zip_archive_file_stat file_stat;
			mz_zip_reader_file_stat(&archive.archive, i, &file_stat);
			if (file_stat.m_is_directory) {
				continue;
			}
			auto file_path = string(file_stat.m_filename);
			if (StringUtil::StartsWith(file_path, "xl/worksheets/") && StringUtil::EndsWith(file_path, ".xml")) {
				// Found a sheet file
				result->sheet_file_idx = i;
				result->sheet_name = file_path.substr(15, file_path.size() - 19);
				break;
			}
		}
	} else {
		// Find the sheet with the given name
		auto sheet_path = "xl/worksheets/" + result->sheet_name + ".xml";
		result->sheet_file_idx = mz_zip_reader_locate_file(&archive.archive, sheet_path.c_str(), nullptr, 0);
	}

	if (result->sheet_file_idx < 0) {
		throw BinderException("Failed to find sheet with name %s", result->sheet_name.c_str());
	}

	// Get the shared strings file index
	result->shared_string_file_idx = mz_zip_reader_locate_file(&archive.archive, "xl/sharedStrings.xml", nullptr, 0);
	if (result->shared_string_file_idx < 0) {
		throw InvalidInputException("Failed to find shared strings file in xlsx archive");
	}

	XLSXSniffer sniffer(HeaderMode::AUTO);
	sniffer.has_start_row = false;
	sniffer.has_start_column = false;
	sniffer.has_end_column = false;

	bool treat_all_fields_as_varchar = false;
	// Get the range (if any)
	for (auto &kv : input.named_parameters) {
		if (kv.first == "row_start") {
			sniffer.has_start_row = true;
			sniffer.start_row = kv.second.GetValue<int>();
		}
		if (kv.first == "column_start") {
			sniffer.has_start_column = true;
			sniffer.start_column = kv.second.GetValue<int>();
		}
		if (kv.first == "column_end") {
			sniffer.has_end_column = true;
			sniffer.end_column = kv.second.GetValue<int>() + 1;
		}
		if (kv.first == "header") {
			auto header_mode = kv.second.GetValue<string>();
			if (StringUtil::Lower(header_mode) == "always") {
				sniffer.header_mode = HeaderMode::ALWAYS;
			} else if (StringUtil::Lower(header_mode) == "never") {
				sniffer.header_mode = HeaderMode::NEVER;
			} else if (StringUtil::Lower(header_mode) == "auto") {
				sniffer.header_mode = HeaderMode::AUTO;
			} else {
				throw BinderException("Invalid header mode: %s", header_mode.c_str());
			}
		}
		if (kv.first == "abort_on_empty_row") {
			result->abort_on_empty_row = kv.second.GetValue<bool>();
		}
		if (kv.first == "all_varchar") {
			treat_all_fields_as_varchar = kv.second.GetValue<bool>();
		}
	}

	if (sniffer.has_start_column && sniffer.has_end_column && sniffer.start_column >= sniffer.end_column) {
		throw BinderException("Specified end column %d is less than or equal to the start column %d",
		                      sniffer.end_column - 1, sniffer.start_column - 1);
	}

	StreamToXMLMachine(sniffer, archive.archive, result->sheet_file_idx);

	if (sniffer.last_column_index < sniffer.end_column) {
		throw BinderException("Specified end column %d is greater than the number of columns in the sheet %d",
		                      sniffer.end_column - 1, sniffer.last_column_index - 1);
	}

	result->range_row_start = sniffer.start_row;
	result->range_column_start = sniffer.start_column;
	result->range_column_end = sniffer.end_column;

	if (sniffer.found_header) {
		// Skip the header row
		result->range_row_start += 1;
	}

	// Resolve any shared strings we need
	SparseStringTableParser string_table_parser;

	for (idx_t i = 0; i < sniffer.column_text.size(); i++) {
		auto &cell_text = sniffer.column_text[i];
		auto &cell_type = sniffer.column_types[i];
		if (cell_type == CellType::SHARED_STRING) {
			auto shared_string_idx = atoi(cell_text.c_str());
			string_table_parser.requested_string_indices.insert(shared_string_idx);
		}
	}
	for (idx_t i = 0; i < sniffer.header_text.size(); i++) {
		auto &cell_text = sniffer.header_text[i];
		auto &cell_type = sniffer.header_types[i];
		if (cell_type == CellType::SHARED_STRING) {
			auto shared_string_idx = atoi(cell_text.c_str());
			string_table_parser.requested_string_indices.insert(shared_string_idx);
		}
	}

	StreamToXMLMachine(string_table_parser, archive.archive, result->shared_string_file_idx);

	// Now bind the actual function
	// Start with the column names
	if (sniffer.found_header) {
		// We found a header, use it to name the column
		for (idx_t i = 0; i < sniffer.header_text.size(); i++) {
			auto &header_text = sniffer.header_text[i];
			auto &header_type = sniffer.header_types[i];
			if (header_type == CellType::SHARED_STRING) {
				auto shared_string_idx = atoi(header_text.c_str());
				auto entry = string_table_parser.string_table.find(shared_string_idx);
				if (entry == string_table_parser.string_table.end()) {
					throw IOException("Failed to find shared string with index %d", shared_string_idx);
				}
				header_text = entry->second;
			}
			names.push_back(header_text);
		}
	} else {
		// Else, use the column indices
		for (idx_t i = 0; i < sniffer.column_text.size(); i++) {
			names.push_back("col" + std::to_string(i));
		}
	}

	// Now the return types
	if (treat_all_fields_as_varchar) {
		return_types = vector<LogicalType>(sniffer.column_text.size(), LogicalType::VARCHAR);
	} else {
		// Use the types from the sniffer
		for (idx_t i = 0; i < sniffer.column_text.size(); i++) {
			auto &cell_type = sniffer.column_types[i];
			return_types.push_back(CellTypes::GetType(cell_type));
		}
	}

	result->column_types = return_types;

	return std::move(result);
}

//-----------------------------------------------------------------------------
// Init Global
//-----------------------------------------------------------------------------

struct SheetParser : public XMLStateMachine<SheetParser> {

	vector<string> string_table;
	vector<char> char_buffer;
	CellType current_cell_type;

	bool in_row = false;
	bool in_col = false;
	bool in_value = false;

	bool in_row_range = false;
	bool in_col_range = false;

	idx_t current_row = 0;
	idx_t current_col = 0;

	idx_t range_row_start;
	idx_t range_column_start;
	idx_t range_column_end;

	idx_t read_row_count = 0;

	bool row_had_data = false;
	bool abort_on_empty_row = true;

	DataChunk payload_chunk;

	void OnStartElement(const char *name, const char **atts) {
		if (!in_row && strcmp(name, "row") == 0) {
			in_row = true;
			if (read_row_count >= STANDARD_VECTOR_SIZE) {
				// We're done for now, yield the chunk
				Suspend();
				return;
			}
			in_row_range = current_row >= range_row_start;
			return;
		}
		if (in_row && strcmp(name, "c") == 0) {
			in_col = true;
			in_col_range = current_col >= range_column_start && current_col < range_column_end;
			if (in_row_range && in_col_range) {
				current_cell_type = GetCellType(atts);
			}
			return;
		}
		if (in_col_range && in_row_range && strcmp(name, "v") == 0) {
			in_value = true;
			return;
		}
	}

	void OnEndElement(const char *name) {
		if (in_row && strcmp(name, "row") == 0) {
			in_row = false;
			current_row++;
			current_col = 0;
			if (in_row_range) {
				if (!row_had_data && abort_on_empty_row) {
					// This row had no data. Abort
					Stop();
					return;
				}
				read_row_count++;
				row_had_data = false;
			}
			return;
		}
		if (in_col && strcmp(name, "c") == 0) {
			in_col = false;

			if (in_row_range && in_col_range) {
				// We're in range, add the value to the payload chunk
				auto col_idx = current_col - range_column_start;
				auto row_idx = read_row_count;
				auto &output_vector = payload_chunk.data[col_idx];
				auto output_data_ptr = FlatVector::GetData<string_t>(output_vector);

				if (char_buffer.empty()) {
					FlatVector::SetNull(output_vector, row_idx, true);
				} else if (current_cell_type == CellType::SHARED_STRING) {
					row_had_data = true;
					auto shared_string_idx = atoi(string(char_buffer.data(), char_buffer.size()).c_str());
					auto entry = string_table[shared_string_idx];
					output_data_ptr[row_idx] = StringVector::AddString(output_vector, entry);
				} else {
					row_had_data = true;
					output_data_ptr[row_idx] =
					    StringVector::AddString(output_vector, char_buffer.data(), char_buffer.size());
				}

				// Reset the buffer
				char_buffer.clear();
			}

			current_col++;
			return;
		}
		if (in_value && strcmp(name, "v") == 0) {
			in_value = false;
			return;
		}
	}

	void OnCharacterData(const char *s, int len) {
		if (in_value) {
			char_buffer.insert(char_buffer.end(), s, s + len);
		}
	}

	enum class State {
		READING,
		PARSING,
		DONE,
	};

	State machine_state = State::READING;

	DataChunk &GetPayloadChunk() {
		return payload_chunk;
	}

	data_t buffer[4096];
	idx_t last_read_size;
	idx_t read_byte_size;
	idx_t total_byte_size;
	mz_zip_reader_extract_iter_state *mz_state;

	~SheetParser() {
		mz_zip_reader_extract_iter_free(mz_state);
	}

	idx_t Execute() {
		read_row_count = 0;
		payload_chunk.Reset();

		while (true) {
			switch (machine_state) {
			case State::READING: {
				auto read_bytes = mz_zip_reader_extract_iter_read(mz_state, buffer, sizeof(buffer));
				read_byte_size += read_bytes;
				last_read_size = read_bytes;
				machine_state = State::PARSING;
			} break;
			case State::PARSING: {
				bool is_final = read_byte_size == total_byte_size;
				auto status = Parse(const_char_ptr_cast(buffer), last_read_size, is_final);
				if (status == XMLParseResult::SUSPENDED) {
					// yield!, we've read all STANDARD_VECTOR_SIZE rows for now.
					// But stay in the parse state!
					machine_state = State::PARSING;
					payload_chunk.SetCardinality(read_row_count);
					return read_row_count;
				} else if (status == XMLParseResult::OK) {
					if (is_final) {
						// We are done
						machine_state = State::DONE;
						payload_chunk.SetCardinality(read_row_count);
						return read_row_count;
					} else {
						// We are not done yet, but we need more data!
						// Move over to the read state
						machine_state = State::READING;
					}
				} else if (status == XMLParseResult::ABORTED) {
					// Early abort
					machine_state = State::DONE;
					payload_chunk.SetCardinality(read_row_count);
					return read_row_count;
				} else {
					throw IOException("Failed to parse XML file");
				}
			} break;
			case State::DONE: {
				payload_chunk.SetCardinality(read_row_count);
				return read_row_count;
			}
			}
		}
	}
};

struct XLSXReaderGlobalState : public GlobalTableFunctionState {
	unique_ptr<MzZipArchive> archive;
	idx_t current_row;
	unique_ptr<SheetParser> sheet_parser;
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<XLSXReaderBindData>();

	auto result = make_uniq<XLSXReaderGlobalState>();
	result->current_row = 0;

	auto &fs = FileSystem::GetFileSystem(context);
	result->archive = make_uniq<MzZipArchive>(fs.OpenFile(bind_data.file_name, FileFlags::FILE_FLAGS_READ));

	// Load the global shared string table
	DenseStringTableParser string_table_parser;
	StreamToXMLMachine(string_table_parser, result->archive->archive, bind_data.shared_string_file_idx);

	// Create state machine
	auto mz_state = mz_zip_reader_extract_iter_new(&result->archive->archive, bind_data.sheet_file_idx, 0);

	result->sheet_parser = make_uniq<SheetParser>();
	result->sheet_parser->mz_state = mz_state;
	vector<LogicalType> payload_types(bind_data.column_types.size(), LogicalType::VARCHAR);
	result->sheet_parser->payload_chunk.Initialize(context, payload_types);
	result->sheet_parser->string_table = std::move(string_table_parser.string_table);
	result->sheet_parser->range_row_start = bind_data.range_row_start;
	result->sheet_parser->range_column_start = bind_data.range_column_start;
	result->sheet_parser->range_column_end = bind_data.range_column_end;
	result->sheet_parser->total_byte_size = mz_state->file_stat.m_uncomp_size;
	result->sheet_parser->abort_on_empty_row = bind_data.abort_on_empty_row;

	return std::move(result);
}

//-----------------------------------------------------------------------------
// Execute
//-----------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<XLSXReaderGlobalState>();

	auto count = global_state.sheet_parser->Execute();
	auto &payload = global_state.sheet_parser->GetPayloadChunk();

	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		if (output.data[i].GetType() == payload.data[i].GetType()) {
			// Types are the same, just move the data
			output.data[i].Reference(payload.data[i]);
		} else {
			// Else, cast the data
			VectorOperations::DefaultCast(payload.data[i], output.data[i], count);
		}
	}

	output.SetCardinality(count);
}

//-----------------------------------------------------------------------------
// Replacement Scan
//-----------------------------------------------------------------------------
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

//-----------------------------------------------------------------------------
// Register
//-----------------------------------------------------------------------------
void XLSXReader::Register(DatabaseInstance &db) {

	TableFunction xlsx_reader("read_xlsx", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal);
	xlsx_reader.named_parameters["sheet"] = LogicalType::VARCHAR;
	xlsx_reader.named_parameters["row_start"] = LogicalType::INTEGER;
	xlsx_reader.named_parameters["column_start"] = LogicalType::INTEGER;
	xlsx_reader.named_parameters["column_end"] = LogicalType::INTEGER;
	xlsx_reader.named_parameters["header"] = LogicalType::VARCHAR;
	xlsx_reader.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	xlsx_reader.named_parameters["abort_on_empty_row"] = LogicalType::BOOLEAN;

	ExtensionUtil::RegisterFunction(db, xlsx_reader);
	db.config.replacement_scans.emplace_back(ReplacementScan);
}

} // namespace duckdb

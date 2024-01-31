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
#include "xlsx_reader.hpp"

namespace duckdb {
//-----------------------------------------------------------------------------
// Parsing
//-----------------------------------------------------------------------------

struct CentralDirectoryFileHeader {
	uint32_t signature;
	uint16_t version_made_by;
	uint16_t version_needed_to_extract;
	uint16_t general_purpose_bit_flag;
	uint16_t compression_method;
	uint16_t last_mod_file_time;
	uint16_t last_mod_file_date;
	uint32_t crc32;
	uint32_t compressed_size;
	uint32_t uncompressed_size;
	uint16_t file_name_length;
	uint16_t extra_field_length;
	uint16_t file_comment_length;
	uint16_t disk_number_start;
	uint16_t internal_file_attributes;
	uint32_t external_file_attributes;
	uint32_t relative_offset_of_local_header;
	string file_name;
	string extra_field;
	string file_comment;

	static CentralDirectoryFileHeader Read(ReadStream &stream) {
		CentralDirectoryFileHeader header;
		stream.ReadData(data_ptr_cast(&header.signature), sizeof(header.signature));
		stream.ReadData(data_ptr_cast(&header.version_made_by), sizeof(header.version_made_by));
		stream.ReadData(data_ptr_cast(&header.version_needed_to_extract), sizeof(header.version_needed_to_extract));
		stream.ReadData(data_ptr_cast(&header.general_purpose_bit_flag), sizeof(header.general_purpose_bit_flag));
		stream.ReadData(data_ptr_cast(&header.compression_method), sizeof(header.compression_method));
		stream.ReadData(data_ptr_cast(&header.last_mod_file_time), sizeof(header.last_mod_file_time));
		stream.ReadData(data_ptr_cast(&header.last_mod_file_date), sizeof(header.last_mod_file_date));
		stream.ReadData(data_ptr_cast(&header.crc32), sizeof(header.crc32));
		stream.ReadData(data_ptr_cast(&header.compressed_size), sizeof(header.compressed_size));
		stream.ReadData(data_ptr_cast(&header.uncompressed_size), sizeof(header.uncompressed_size));
		stream.ReadData(data_ptr_cast(&header.file_name_length), sizeof(header.file_name_length));
		stream.ReadData(data_ptr_cast(&header.extra_field_length), sizeof(header.extra_field_length));
		stream.ReadData(data_ptr_cast(&header.file_comment_length), sizeof(header.file_comment_length));
		stream.ReadData(data_ptr_cast(&header.disk_number_start), sizeof(header.disk_number_start));
		stream.ReadData(data_ptr_cast(&header.internal_file_attributes), sizeof(header.internal_file_attributes));
		stream.ReadData(data_ptr_cast(&header.external_file_attributes), sizeof(header.external_file_attributes));
		stream.ReadData(data_ptr_cast(&header.relative_offset_of_local_header),
		                sizeof(header.relative_offset_of_local_header));

		vector<char> buffer(header.file_name_length);
		stream.ReadData(data_ptr_cast(buffer.data()), header.file_name_length);
		header.file_name = string(buffer.data(), header.file_name_length);

		buffer.resize(header.extra_field_length);
		stream.ReadData(data_ptr_cast(buffer.data()), header.extra_field_length);
		header.extra_field = string(buffer.data(), header.extra_field_length);

		buffer.resize(header.file_comment_length);
		stream.ReadData(data_ptr_cast(buffer.data()), header.file_comment_length);
		header.file_comment = string(buffer.data(), header.file_comment_length);

		return header;
	}
};

struct LocalFileHeader {
	uint32_t signature;
	uint16_t version_needed_to_extract;
	uint16_t general_purpose_bit_flag;
	uint16_t compression_method;
	uint16_t last_mod_file_time;
	uint16_t last_mod_file_date;
	uint32_t crc32;
	uint32_t compressed_size;
	uint32_t uncompressed_size;
	uint16_t file_name_length;
	uint16_t extra_field_length;
	string file_name;
	string extra_field;

	static LocalFileHeader Read(ReadStream &stream) {
		LocalFileHeader header;
		stream.ReadData(data_ptr_cast(&header.signature), sizeof(header.signature));
		stream.ReadData(data_ptr_cast(&header.version_needed_to_extract), sizeof(header.version_needed_to_extract));
		stream.ReadData(data_ptr_cast(&header.general_purpose_bit_flag), sizeof(header.general_purpose_bit_flag));
		stream.ReadData(data_ptr_cast(&header.compression_method), sizeof(header.compression_method));
		stream.ReadData(data_ptr_cast(&header.last_mod_file_time), sizeof(header.last_mod_file_time));
		stream.ReadData(data_ptr_cast(&header.last_mod_file_date), sizeof(header.last_mod_file_date));
		stream.ReadData(data_ptr_cast(&header.crc32), sizeof(header.crc32));
		stream.ReadData(data_ptr_cast(&header.compressed_size), sizeof(header.compressed_size));
		stream.ReadData(data_ptr_cast(&header.uncompressed_size), sizeof(header.uncompressed_size));
		stream.ReadData(data_ptr_cast(&header.file_name_length), sizeof(header.file_name_length));
		stream.ReadData(data_ptr_cast(&header.extra_field_length), sizeof(header.extra_field_length));

		vector<char> buffer(header.file_name_length);
		stream.ReadData(data_ptr_cast(buffer.data()), header.file_name_length);
		header.file_name = string(buffer.data(), header.file_name_length);

		buffer.resize(header.extra_field_length);
		stream.ReadData(data_ptr_cast(buffer.data()), header.extra_field_length);
		header.extra_field = string(buffer.data(), header.extra_field_length);

		return header;
	}
};

#define MZ_STREAM_SIZE 4096
struct MzStream : ReadStream {
	duckdb_miniz::mz_stream stream;
	data_t buffer[MZ_STREAM_SIZE];
	ReadStream &reader;

	explicit MzStream(ReadStream &reader) : reader(reader) {
		memset(&stream, 0, sizeof(stream));
		auto mz_ret = duckdb_miniz::mz_inflateInit2(&stream, -MZ_DEFAULT_WINDOW_BITS);
		if (mz_ret != duckdb_miniz::MZ_OK) {
			throw IOException("Failed to initialize miniz");
		}
	}

	void ReadData(data_ptr_t dst, idx_t read_size) override {
		stream.next_out = dst;
		stream.avail_out = read_size;
		while (stream.avail_out > 0) {
			if (stream.avail_in == 0) {
				reader.ReadData(buffer, 4096);
				stream.next_in = buffer;
				stream.avail_in = 4096;
			}
			auto mz_ret = duckdb_miniz::mz_inflate(&stream, duckdb_miniz::MZ_NO_FLUSH);
			if (mz_ret != duckdb_miniz::MZ_OK && mz_ret != duckdb_miniz::MZ_STREAM_END) {
				throw IOException("Failed to inflate miniz");
			}

			// TODO: Read in chunks
			stream.next_out += read_size;
			stream.avail_out = 0;
		}
	}
};

#define EXCEL_BUFFER_SIZE 4096
struct ExcelParseState {
	data_t buffer[EXCEL_BUFFER_SIZE];
	unique_ptr<ReadStream> stream;
	XML_Parser parser;

	explicit ExcelParseState(unique_ptr<ReadStream> stream)
	    : stream(std::move(stream)), parser(XML_ParserCreate(nullptr)) {
	}

	void Next() {
		stream->ReadData(buffer, EXCEL_BUFFER_SIZE);
		XML_Parse(parser, const_char_ptr_cast(buffer), EXCEL_BUFFER_SIZE, 0);
	}
};

//-----------------------------------------------------------------------------
// Bind
//-----------------------------------------------------------------------------
struct XLSXReaderBindData : public TableFunctionData {
	// todo
	string file_name;
	string sheet_name;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<XLSXReaderBindData>();
		return file_name == other.file_name && sheet_name == other.sheet_name;
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XLSXReaderBindData>();

	result->file_name = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "sheet") {
			result->sheet_name = kv.second.GetValue<string>();
		}
	}

	auto &fs = FileSystem::GetFileSystem(context);
	/*
	auto file = fs.OpenFile(result->file_name, FileFlags::FILE_FLAGS_READ);
	if (!file) {
	    throw IOException("Failed to open file %s", result->file_name.c_str());
	}
	 */

	// try to read the file
	BufferedFileReader reader(fs, result->file_name.c_str());

	// lets keep it simple and pretend that the OECD is at the end of the file (no comment)
	reader.Seek(reader.FileSize() - 22);
	data_t oecd_buffer[22];
	reader.ReadData(oecd_buffer, 22);

	uint32_t signature;
	uint16_t disk_number;
	uint16_t disk_number_with_start_of_central_directory;
	uint16_t number_of_entries_in_central_directory_on_this_disk;
	uint16_t number_of_entries_in_central_directory;
	uint32_t size_of_central_directory;
	uint32_t offset_of_start_of_central_directory;
	uint16_t comment_length;

	memcpy(&signature, oecd_buffer, 4);
	memcpy(&disk_number, oecd_buffer + 4, 2);
	memcpy(&disk_number_with_start_of_central_directory, oecd_buffer + 6, 2);
	memcpy(&number_of_entries_in_central_directory_on_this_disk, oecd_buffer + 8, 2);
	memcpy(&number_of_entries_in_central_directory, oecd_buffer + 10, 2);
	memcpy(&size_of_central_directory, oecd_buffer + 12, 4);
	memcpy(&offset_of_start_of_central_directory, oecd_buffer + 16, 4);
	memcpy(&comment_length, oecd_buffer + 20, 2);

	if (signature != 0x06054b50) {
		throw IOException("Invalid signature");
	}
	if (disk_number != disk_number_with_start_of_central_directory) {
		throw IOException("DuckDB does not support multi-disk archives");
	}
	if (number_of_entries_in_central_directory_on_this_disk != number_of_entries_in_central_directory) {
		throw IOException("DuckDB does not support multi-disk archives");
	}

	// Now start reading the central directory file headers
	reader.Seek(offset_of_start_of_central_directory);

	vector<string> found_sheets;
	bool found_sheet = false;
	LocalFileHeader local_header;
	for (idx_t i = 0; i < number_of_entries_in_central_directory; i++) {
		auto central_header = CentralDirectoryFileHeader::Read(reader);
		auto save_offset = reader.CurrentOffset();

		auto embedded_file_name = central_header.file_name;
		if (embedded_file_name == "xl/sharedStrings.xml") {
			printf("Found shared strings");
		}

		if (StringUtil::StartsWith(embedded_file_name, "xl/worksheets/") &&
		    StringUtil::EndsWith(embedded_file_name, ".xml")) {
			auto sheet_name = embedded_file_name.substr(15, central_header.file_name.size() - 19);
			found_sheets.push_back(sheet_name);

			if (result->sheet_name.empty() || result->sheet_name == sheet_name) {
				printf("Found worksheet: %s\n", embedded_file_name.c_str());
				found_sheet = true;

				// Since we only have one disk, the relative offset is the same as the absolute offset
				reader.Seek(central_header.relative_offset_of_local_header);
				local_header = LocalFileHeader::Read(reader);
				break;
			}
		}
		reader.Seek(save_offset);
	}
	if (!found_sheet) {
		if (found_sheets.empty()) {
			throw IOException("No worksheets found");
		} else {
			throw IOException("Worksheet %s not found, found sheets: %s", result->sheet_name.c_str(),
			                  StringUtil::Join(found_sheets, ", ").c_str());
		}
	}

	unique_ptr<ReadStream> stream;
	if (local_header.compression_method == 8) {
		stream = make_uniq<MzStream>(reader);
	} else if (local_header.compression_method == 0) {
		stream = make_uniq<BufferedFileReader>(std::move(reader));
	} else {
		throw IOException("Unsupported compression method");
	}

	ExcelParseState state(std::move(stream));
	XML_SetElementHandler(
	    state.parser,
	    [](void *user_data, const char *name, const char *args[]) {
		    printf("Begin element: %s\n", name);
		    while (*args) {
			    printf("Arg %s=%s\n", *args, *(args + 1));
			    args += 2;
		    }
	    },
	    [](void *user_data, const char *name) { printf("End element: %s\n", name); });
	XML_SetCharacterDataHandler(state.parser, [](void *user_data, const char *str, int len) {
		printf("Character data: %s\n", string(str, len).c_str());
	});

	state.Next();

	// XML_Parse(parser, str.c_str(), str.size(), 1);
	// XML_ParserFree(parser);

	// After the header is the data in of "compressed_size" bytes

	/*
	vector<data_t> compressed_buffer(local_header.compressed_size);
	reader.ReadData(compressed_buffer.data(), local_header.compressed_size);
	vector<data_t> uncompressed_buffer(local_header.uncompressed_size);

	// Compression method 0 = uncompressed
	// Compression method 8 = deflate
	// deflate the data
	if (local_header.compression_method == 8) {
	    duckdb_miniz::mz_stream stream;
	    memset(&stream, 0, sizeof(stream));
	    auto mz_ret = duckdb_miniz::mz_inflateInit2(&stream, -MZ_DEFAULT_WINDOW_BITS);
	    if (mz_ret != duckdb_miniz::MZ_OK) {
	        throw IOException("Failed to initialize miniz");
	    }

	    stream.next_in = compressed_buffer.data();
	    stream.avail_in = local_header.compressed_size;
	    stream.next_out = uncompressed_buffer.data();
	    stream.avail_out = local_header.uncompressed_size;
	    mz_ret = duckdb_miniz::mz_inflate(&stream, duckdb_miniz::MZ_FINISH);
	    if (mz_ret != duckdb_miniz::MZ_STREAM_END) {
	        throw IOException("Failed to inflate miniz");
	    }
	    mz_ret = duckdb_miniz::mz_inflateEnd(&stream);
	    if (mz_ret != duckdb_miniz::MZ_OK) {
	        throw IOException("Failed to end miniz");
	    }
	} else if (local_header.compression_method == 0) {
	    memcpy(uncompressed_buffer.data(), compressed_buffer.data(), local_header.uncompressed_size);
	} else {
	    throw IOException("Unsupported compression method");
	}
	auto str = string(char_ptr_cast(uncompressed_buffer.data()), local_header.uncompressed_size);
	printf("File name: %s, size: %d\n", local_header.file_name.c_str(), local_header.uncompressed_size);


	// Now parse the XML
	XML_Parser parser = XML_ParserCreate(nullptr);
	//XML_SetUserData(parser, &result->sheet_name);
	XML_SetElementHandler(parser,
	    [](void* user_data, const char* name, const char *args[]) {
	        printf("Begin element: %s\n", name);
	        while(*args) {
	            printf("Arg %s=%s\n", *args, *(args+1));
	            args += 2;
	        }
	    },
	    [](void* user_data, const char* name) {
	        printf("End element: %s\n", name);
	    }
	);
	XML_SetCharacterDataHandler(parser,
	    [](void* user_data, const char* str, int len) {
	        printf("Character data: %s\n", string(str, len).c_str());
	    }
	);

	XML_Parse(parser, str.c_str(), str.size(), 1);
	XML_ParserFree(parser);

	return std::move(result);
	 */

	return std::move(result);
}

//-----------------------------------------------------------------------------
// Init Global
//-----------------------------------------------------------------------------
struct XLSXReaderGlobalData : public GlobalTableFunctionState {};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<XLSXReaderGlobalData>();
	return std::move(result);
}

//-----------------------------------------------------------------------------
// Execute
//-----------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
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

	ExtensionUtil::RegisterFunction(db, xlsx_reader);
	db.config.replacement_scans.emplace_back(ReplacementScan);
}

} // namespace duckdb

//-----------------------------------------------------------------------------
//
//-----------------------------------------------------------------------------

struct BindParseState {
	CellRange dimensions = CellRange();
	bool is_done = false;
	bool in_first_row = false;
	bool in_cell = false;
	bool in_cell_value = false;
	CellRef current_cell = CellRef(0, 0);
	bool has_header = false;
	vector<string> header_values = vector<string>();
	vector<CellType> header_types = vector<CellType>();
};

struct SheetInfo {
	bool has_header = false;
	vector<string> column_names = vector<string>();
	vector<LogicalType> column_types = vector<LogicalType>();
};

static unique_ptr<SheetInfo> GetSheetInfo(mz_zip_archive &archive, int sheet_idx) {
	BindParseState state;

	// Create parser
	auto parser = XML_ParserCreate(nullptr);
	XML_SetUserData(parser, &state);

	XML_SetElementHandler(
	    parser,
	    [](void *user_data, const XML_Char *name, const XML_Char **atts) {
		    auto state = static_cast<BindParseState *>(user_data);
		    /* // TODO: We dont care about this now, could be an optimization later
		    // <dimension> tag contains the dimensions of the sheet
		    // Crap! This tag is optional!!!
		    if(strcmp(name, "dimension") == 0) {
		        // Extract the "ref" attribute value
		        for (int i = 0; atts[i]; i += 2) {
		            if ((strcmp(atts[i], "ref") == 0)) {
		                // Extract the cell range from the "ref" attribute value
		                state->dimensions = CellRange(atts[i + 1]);
		                return;
		            }
		        }
		        return;
		    }
		    */

		    // Check if we are in the first row
		    if (strcmp(name, "row") == 0) {
			    // Extract the "r" attribute value
			    for (int i = 0; atts[i]; i += 2) {
				    if ((strcmp(atts[i], "r") == 0)) {
					    // Extract the row number from the "r" attribute value
					    auto row = atoi(atts[i + 1]) - 1; // 0 index
					    if (row == 0) {
						    state->in_first_row = true;
					    }
				    }
			    }
			    return;
		    }

		    // Inside a column
		    if (state->in_first_row) {
			    if (strcmp(name, "c") == 0) {
				    state->in_cell = true;
				    // Extract the "r" attribute value
				    auto cell_type = CellType::NUMBER;
				    for (int i = 0; atts[i]; i += 2) {
					    if ((strcmp(atts[i], "r") == 0)) {
						    // Extract the cell reference from the "r" attribute value
						    auto ptr = const_cast<char *>(atts[i + 1]);
						    state->current_cell = CellRef(ptr);
					    }

					    // Look for the "t" attribute value
					    if ((strcmp(atts[i], "t") == 0)) {
						    // Extract the cell type from the "t" attribute value
						    cell_type = CellTypes::FromString(atts[i + 1]);
					    }
				    }
				    state->header_types.push_back(cell_type);
				    return;
			    }

			    // Inside a value
			    if (strcmp(name, "v") == 0) {
				    state->in_cell_value = true;
				    return;
			    }
		    }
	    },
	    [](void *user_data, const XML_Char *name) {
		    auto state = static_cast<BindParseState *>(user_data);

		    // Once we exit the first row, we're done
		    if (strcmp(name, "row") == 0) {
			    if (state->in_first_row) {
				    state->in_first_row = false;
				    state->is_done = true;
			    }
			    return;
		    }
		    if (state->in_first_row) {
			    if (strcmp(name, "c") == 0) {
				    state->in_cell = false;
				    return;
			    }

			    if (strcmp(name, "v") == 0) {
				    state->in_cell_value = false;
				    return;
			    }
		    }
	    });

	XML_SetCharacterDataHandler(parser, [](void *user_data, const XML_Char *s, int len) {
		auto &state = *static_cast<BindParseState *>(user_data);
		if (state.in_first_row && state.in_cell_value) {
			// Parse the input buffer
			state.header_values.emplace_back(s, len);
		}
	});

	// Stream the file into the parser, chunk by chunk
	mz_zip_reader_extract_to_callback(
	    &archive, sheet_idx,
	    [](void *user_data, mz_uint64 file_offset, const void *input_buffer, size_t n) {
		    auto parser = *static_cast<XML_Parser *>(user_data);
		    auto state = static_cast<BindParseState *>(XML_GetUserData(parser));
		    if (state->is_done) {
			    // Return 0 to indicate we're done
			    return static_cast<size_t>(0);
		    }
		    // Parse the input buffer
		    XML_Parse(parser, static_cast<const char *>(input_buffer), n, false);

		    // Return the number of bytes we parsed
		    return n;
	    },
	    &parser, 0);
	// Do a final parse to flush the parser
	XML_Parse(parser, nullptr, 0, true);

	// Free the parser
	XML_ParserFree(parser);

	// Now check if the first row is a header
	bool header_detected = true;
	bool header_has_shared_string = false;
	for (auto &cell : state.header_types) {
		if (cell == CellType::SHARED_STRING) {
			header_has_shared_string = true;
			continue;
		}
		if (cell == CellType::STRING) {
			continue;
		}
		// One of the types is not a string, so there is _probably_ no header
		header_detected = false;
		break;
	}

	auto result = make_uniq<SheetInfo>();

	if (header_detected) {
		result->has_header = true;
		if (header_has_shared_string) {
			// We need to scan the shared strings table to get the header values
			unordered_set<idx_t> wanted_indices;
			for (idx_t i = 0; i < state.header_values.size(); i++) {
				auto &cell = state.header_values[i];
				if (state.header_types[i] == CellType::SHARED_STRING) {
					auto index = atoi(cell.c_str());
					wanted_indices.insert(index);
				}
			}

			// Resolve the shared string indices
			unordered_map<idx_t, string> string_table;
			ScanSharedStringsByIndex(string_table, std::move(wanted_indices), archive);

			for (idx_t i = 0; i < state.header_values.size(); i++) {
				auto &cell = state.header_values[i];
				if (state.header_types[i] == CellType::SHARED_STRING) {
					auto index = atoi(cell.c_str());
					result->column_names.push_back(string_table[index]);
				} else {
					result->column_names.push_back(cell);
				}
			}

		} else {
			// We can just use the values we already have
			result->column_names = state.header_values;
		}
	} else {
		result->has_header = false;
		// Make up some column names based on the column indices
		auto column_cells = state.dimensions.GetColumns();
		for (auto &cell : column_cells) {
			result->column_names.push_back(cell.ColumnToString());
		}
	}

	for (auto &cell : state.header_types) {
		result->column_types.push_back(CellTypes::GetType(cell));
	}

	return result;
}

/*
static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<XLSXReaderBindData>();

// Get the file name
result->file_name = input.inputs[0].GetValue<string>();

// Get the sheet name
for(auto &kv : input.named_parameters) {
    if(kv.first == "sheet") {
        result->sheet_name = kv.second.GetValue<string>();
    }
}

auto &fs = FileSystem::GetFileSystem(context);

MzZipArchive archive(fs.OpenFile(result->file_name, FileFlags::FILE_FLAGS_READ));

// Now get the sheet dimensions and columns
// Get the index of the sheet
result->sheet_file_idx = -1;
if(result->sheet_name.empty()) {
    // No sheet name specified, loop over the files and find the first sheet
    for(auto i = 0; i < mz_zip_reader_get_num_files(&archive.archive); i++) {
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

if(result->sheet_file_idx < 0) {
    throw BinderException("Failed to find sheet with name %s", result->sheet_name.c_str());
}

auto info = GetSheetInfo(archive.archive, result->sheet_file_idx);

result->column_count = info->column_names.size();
// Bind the columns
names = info->column_names;
return_types = info->column_types;
return std::move(result);
}
*/

struct FetchStringsState {
	bool is_done = false;
	bool in_t_tag = false;
	idx_t current_string_idx = 0;
	unordered_set<idx_t> requested_string_indices;

	// Result data
	unordered_map<idx_t, string> &string_table;
	explicit FetchStringsState(unordered_map<idx_t, string> &string_table_p, unordered_set<idx_t> indices_p)
	    : requested_string_indices(std::move(indices_p)), string_table(string_table_p) {
	}
};

static void ScanSharedStringsByIndex(unordered_map<idx_t, string> &result_string_table, unordered_set<idx_t> indices,
                                     mz_zip_archive &archive) {
	// Find the shared string table
	auto string_table_idx = mz_zip_reader_locate_file(&archive, "xl/sharedStrings.xml", nullptr, 0);
	if (string_table_idx < 0) {
		throw IOException("Failed to find shared string table");
	}

	FetchStringsState state(result_string_table, std::move(indices));

	// Create parser
	auto parser = XML_ParserCreate(nullptr);
	XML_SetUserData(parser, &state);

	XML_SetElementHandler(
	    parser,
	    [](void *user_data, const XML_Char *name, const XML_Char **atts) {
		    auto state = static_cast<FetchStringsState *>(user_data);
		    if (strcmp(name, "t") == 0) {
			    state->in_t_tag = true;
			    return;
		    }
	    },
	    [](void *user_data, const XML_Char *name) {
		    auto state = static_cast<FetchStringsState *>(user_data);
		    if (strcmp(name, "t") == 0) {
			    state->in_t_tag = false;
			    state->current_string_idx++;
			    return;
		    }
	    });
	XML_SetCharacterDataHandler(parser, [](void *user_data, const XML_Char *s, int len) {
		auto &state = *static_cast<FetchStringsState *>(user_data);
		if (state.in_t_tag) {
			auto current_idx = state.current_string_idx;
			if (state.requested_string_indices.find(current_idx) == state.requested_string_indices.end()) {
				// We don't care about this string
				return;
			} else {
				// We do care about this string
				state.requested_string_indices.erase(current_idx);
				state.string_table[current_idx] = string(s, len);

				if (state.requested_string_indices.empty()) {
					// We're done
					state.is_done = true;
				}
			}
		}
	});

	// Stream the file into the parser, chunk by chunk
	mz_zip_reader_extract_to_callback(
	    &archive, string_table_idx,
	    [](void *user_data, mz_uint64 file_offset, const void *input_buffer, size_t n) {
		    auto parser = *static_cast<XML_Parser *>(user_data);
		    auto state = static_cast<FetchStringsState *>(XML_GetUserData(parser));
		    if (state->is_done) {
			    // Return 0 to indicate we're done
			    return static_cast<size_t>(0);
		    }
		    // Parse the input buffer
		    XML_Parse(parser, static_cast<const char *>(input_buffer), n, false);

		    // Return the number of bytes we parsed
		    return n;
	    },
	    &parser, 0);

	// Do a last parse to flush the parser
	XML_Parse(parser, nullptr, 0, true);

	// Free the parser
	XML_ParserFree(parser);
}

/*
struct StateMachine {

idx_t total_byte_size;
idx_t read_byte_size;
idx_t read_row_count;
DataChunk payload_chunk;
vector<string> *string_table;

// Parser state
bool in_row_tag;
bool in_cell_tag;
bool in_cell_value_tag;
idx_t current_row;
idx_t current_col;
CellType current_cell_type;

data_t buffer[4096];
idx_t last_read_size;

State machine_state;

mz_zip_reader_extract_iter_state *mz_state;
XML_Parser parser;

// Takes ownership of the mz_state
explicit StateMachine(ClientContext &context, mz_zip_reader_extract_iter_state* mz_state_p, idx_t column_count) {
    machine_state = State::READING;
    read_byte_size = 0;
    read_row_count = 0;
    last_read_size = 0;
    parser = XML_ParserCreate(nullptr);
    mz_state = mz_state_p;

    total_byte_size = mz_state->file_stat.m_uncomp_size;

    in_row_tag = false;
    in_cell_tag = false;
    in_cell_value_tag = false;
    current_row = 0;
    current_col = 0;
    current_cell_type = CellType::NUMBER;

    // We use a payload chunk to store the parsed data
    payload_chunk.Initialize(context, vector<LogicalType>(column_count, LogicalType::VARCHAR));

    // Set the user data to this state machine
    XML_SetUserData(parser, this);

    // Handle tags
    XML_SetElementHandler(parser,
        [](void *user_data, const XML_Char *name, const XML_Char **atts){
            auto &state = *static_cast<StateMachine*>(user_data);
            if(strcmp(name, "row") == 0) {
                state.in_row_tag = true;
                return;
            }
            if(strcmp(name, "c") == 0) {
                state.in_cell_tag = true;
                // Default to number
                state.current_cell_type = CellType::NUMBER;

                // Extract the "t" attribute value
                for (int i = 0; atts[i]; i += 2) {
                    if ((strcmp(atts[i], "t") == 0)) {
                        // Extract the cell type from the "t" attribute value
                        state.current_cell_type = CellTypes::FromString(atts[i + 1]);
                    }
                }
                return;
            }
            if(strcmp(name, "v") == 0) {
                state.in_cell_value_tag = true;
                return;
            }
        },
        [](void *user_data, const XML_Char *name){
            auto &state = *static_cast<StateMachine*>(user_data);
            if(strcmp(name, "row") == 0) {
                state.in_row_tag = false;
                state.read_row_count++;
                state.current_row++;
                state.current_col = 0;

                if(state.read_row_count >= STANDARD_VECTOR_SIZE) {
                    // We've read enough rows, suspend the parser
                    XML_StopParser(state.parser, true);
                }
                return;
            }
            if(strcmp(name, "c") == 0) {
                state.in_cell_tag = false;
                state.current_col++;
                return;
            }
            if(strcmp(name, "v") == 0) {
                state.in_cell_value_tag = false;
                return;
            }
        });

    // Handle character data
    XML_SetCharacterDataHandler(parser, [](void *user_data, const XML_Char *s, int len){
        auto &state = *static_cast<StateMachine*>(user_data);
        if(state.in_cell_value_tag) {
            // Parse the input buffer
            auto &output_vector = state.payload_chunk.data[state.current_col];
            auto output_data_ptr = FlatVector::GetData<string_t>(output_vector);

            if(state.current_cell_type == CellType::SHARED_STRING) {
                auto shared_string_idx = atoi(string(s, len).c_str());
                auto shared_string = state.string_table->at(shared_string_idx);
                output_data_ptr[state.read_row_count] = StringVector::AddString(output_vector, shared_string);
            } else {
                // Just use the raw string value for now until we get proper types.
                // Actually we will do that through casting later.
                // TODO: Setup a payload chunk with the column types from the binding
                output_data_ptr[state.read_row_count] = StringVector::AddString(output_vector, string_t(s, len));
            }
        }
    });
}

~StateMachine() {
    mz_zip_reader_extract_iter_free(mz_state);
    XML_ParserFree(parser);
}

DataChunk &GetPayloadChunk() {
    return payload_chunk;
}

idx_t Run() {
    // We reset the row count here
    payload_chunk.Reset();
    read_row_count = 0;

    while(true) {
        switch (machine_state) {
        case State::READING: {
            auto read_bytes = mz_zip_reader_extract_iter_read(mz_state, buffer, sizeof(buffer));
            read_byte_size += read_bytes;
            last_read_size = read_bytes;
            machine_state = State::PARSING;
        } break;
        case State::PARSING: {
            bool is_final = read_byte_size == total_byte_size;
            auto status = XML_Parse(parser, const_char_ptr_cast(buffer), last_read_size, is_final);
            if (status == XML_STATUS_SUSPENDED) {
                // yield!, we've read all STANDARD_VECTOR_SIZE rows for now.
                // But stay in the parse state!
                machine_state = State::PARSING;
                payload_chunk.SetCardinality(read_row_count);
                return read_row_count;
            } else if (status == XML_STATUS_OK) {
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
*/

/*
 *
 *
template<class T>
class Optional {
    static_assert(std::is_trivial<T>::value, "T must be trivial");
private:
    bool is_set;
    T value;
public:
    Optional() : is_set(false) {}
    Optional(const T &value_p) : is_set(true), value(value_p) {}
    Optional(const Optional &other) : is_set(other.is_set), value(other.value) {}
    Optional &operator=(const Optional &other) {
        is_set = other.is_set;
        value = other.value;
        return *this;
    }
    Optional &operator=(const T &value_p) {
        is_set = true;
        value = value_p;
        return *this;
    }

bool IsSome() const {
    return is_set;
}

bool IsNone() const {
    return !is_set;
}

const T &Get() const {
    D_ASSERT(is_set);
    return value;
}
};

*/

/*

class XLSXFileHandle {
    unique_ptr<FileHandle> file_handle;
public:
    mz_zip_archive zip_archive;
    explicit XLSXFileHandle(unique_ptr<FileHandle> file_handle_p) : file_handle(std::move(file_handle_p)) {
        mz_zip_zero_struct(&zip_archive);

zip_archive.m_pIO_opaque = file_handle.get();
zip_archive.m_pRead = [](void *user_data, mz_uint64 file_offset, void* buffer, size_t n) {
    auto handle = static_cast<FileHandle *>(user_data);
    handle->Seek(file_offset);
    auto bytes = handle->Read(buffer, n);
    return static_cast<size_t>(bytes);
};

if(!mz_zip_reader_init(&zip_archive, file_handle->GetFileSize(), MZ_ZIP_FLAG_COMPRESSED_DATA)) {
    auto error = mz_zip_get_last_error(&zip_archive);
    auto error_str = mz_zip_get_error_string(error);
    throw IOException("Failed to initialize zip archive: %s", error_str);
}
}

int GetSheetFileIndexByName(const string &sheet_name) {
    auto file_path = "xl/worksheets/" + sheet_name + ".xml";
    auto file_idx = mz_zip_reader_locate_file(&zip_archive, file_path.c_str(), nullptr, 0);
    if(file_idx < 0) {
        auto alternatives = GetAllAvailableSheets();
        auto candidate_msg = StringUtil::CandidatesErrorMessage(alternatives, sheet_name, "Candidate sheets");
        throw IOException("Failed to find sheet %s in xlsx file\n%s", sheet_name, candidate_msg);
    }
    return file_idx;
}

int GetStringDictionaryFileIndex() {
    auto file_idx = mz_zip_reader_locate_file(&zip_archive, "xl/sharedStrings.xml", nullptr, 0);
    if(file_idx < 0) {
        throw IOException("Failed to find string dictionary in xlsx file (is the file corrupt?)");
    }
    return file_idx;
}

std::pair<string, int> GetFirstAvailableSheet() {
    for(mz_uint i = 0; i < mz_zip_reader_get_num_files(&zip_archive); i++) {
        mz_zip_archive_file_stat file_stat;
        mz_zip_reader_file_stat(&zip_archive, i, &file_stat);
        if (file_stat.m_is_directory) {
            continue;
        }
        auto file_path = string(file_stat.m_filename);
        if (StringUtil::StartsWith(file_path, "xl/worksheets/") && StringUtil::EndsWith(file_path, ".xml")) {
            // Found a sheet file
            auto file_idx = i;
            auto sheet_name = file_path.substr(14, file_path.size() - 18);
            return std::make_pair(sheet_name, file_idx);
        }
    }
    throw IOException("Failed to find any sheets in xlsx file (is the file corrupt?)");
}

vector<string> GetAllAvailableSheets() {
    vector<string> result;
    for(mz_uint i = 0; i < mz_zip_reader_get_num_files(&zip_archive); i++) {
        mz_zip_archive_file_stat file_stat;
        mz_zip_reader_file_stat(&zip_archive, i, &file_stat);
        if (file_stat.m_is_directory) {
            continue;
        }
        auto file_path = string(file_stat.m_filename);
        if (StringUtil::StartsWith(file_path, "xl/worksheets/") && StringUtil::EndsWith(file_path, ".xml")) {
            // Found a sheet file
            auto sheet_name = file_path.substr(15, file_path.size() - 19);
            result.push_back(sheet_name);
        }
    }
    return result;
}

struct IterStateDeleter {
    void operator()(mz_zip_reader_extract_iter_state *state) {
        mz_zip_reader_extract_iter_free(state);
    }
};
using IterStatePtr = std::unique_ptr<mz_zip_reader_extract_iter_state, IterStateDeleter>;


template<class T>
void ParseSheetWithMachineUntilCompletion(int sheet_idx, XMLStateMachine<T> &machine) {
    mz_zip_archive_file_stat file_stat;
    auto ok = mz_zip_reader_file_stat(&zip_archive, sheet_idx, &file_stat);
    if(!ok) {
        throw IOException("Failed to read file stat for sheet %d", sheet_idx);
    }

    auto iter = IterStatePtr(mz_zip_reader_extract_iter_new(&zip_archive, sheet_idx, 0));
    if(!iter) {
        throw IOException("Failed to initialize extraction iterator for sheet %d", sheet_idx);
    }

    mz_uint64 bytes_total = file_stat.m_uncomp_size;
    mz_uint64 bytes_read = 0;

    char buffer[2048];
    while(bytes_read != bytes_total) {
        auto read_size = mz_zip_reader_extract_iter_read(iter.get(), buffer, sizeof(buffer));
        bytes_read += read_size;
        auto status = machine.Parse(buffer, read_size, bytes_read == bytes_total);
        while(status == XMLParseResult::SUSPENDED) {
            status = machine.Resume();
        }
        if(status == XMLParseResult::ABORTED) {
            return;
        }
    }
}

~XLSXFileHandle() {
    mz_zip_reader_end(&zip_archive);
}
};
 */
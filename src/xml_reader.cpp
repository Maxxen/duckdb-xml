#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "xml_reader.hpp"
#include "expat.h"
#include "xml_state_machine.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Sniff
//------------------------------------------------------------------------------


enum class XMLTableLayout {
	ROWS,
	/*
	 	<row column1="value1" column2="value2" .../>
	 */
	COLUMNS,
	/*
		<row>
			<column1>value1</column1>
			<column2>value2</column2>
		</row>
	 */

	FIELDS,
	/*
		<row>
			<field name='column1'>value1</field>
			<field name='column2'>value2</field>
		</row>
	 */
};

struct XMLSniffResult {
	XMLTableLayout table_layout;
	vector<string> column_names;
	vector<LogicalType> column_types;
};

static XMLSniffResult SniffXLSX(FileHandle &handle, const string &row_tag) {
	auto parser = XML_ParserCreate(nullptr);

	struct SniffState {
		const char* row_tag = nullptr;
		bool in_row_tag = false;
		XML_Parser parser;

		bool in_column_tag = false;
		vector<string> column_tags;
		vector<string> column_text;
		vector<map<string, string>> column_attributes;
		map<string, string> row_attributes;
	};

	SniffState state;
	state.row_tag = row_tag.c_str();
	state.parser = parser;

	XML_SetUserData(parser, &state);

	XML_SetElementHandler(parser, [](void *user_data, const XML_Char *name, const XML_Char **atts) {
		auto &state = *reinterpret_cast<SniffState *>(user_data);
		if(!state.in_row_tag && strcmp(name, state.row_tag) == 0) {
			// Opening row tag
			state.in_row_tag = true;

			// Read attributes
			state.row_attributes.clear();
			for(size_t i = 0; atts[i]; i += 2) {
				state.row_attributes[atts[i]] = atts[i + 1];
			}
			return;
		}
		if(state.in_row_tag && !state.in_column_tag) {
			// Opening a column tag
			state.in_column_tag = true;
			state.column_tags.push_back(name);
			state.column_text.emplace_back();
			state.column_attributes.emplace_back();

			// Read attributes
			auto &column_attributes = state.column_attributes.back();
			for(size_t i = 0; atts[i]; i += 2) {
				column_attributes[atts[i]] = atts[i + 1];
			}
			return;
		}
	}, [](void *user_data, const XML_Char *name) {
		auto &state = *reinterpret_cast<SniffState *>(user_data);
		if(state.in_row_tag && strcmp(name, state.row_tag) == 0) {
			// Closing row tag
			state.in_row_tag = false;

			// We read a row, now we're done, abort the parse.
			XML_StopParser(state.parser, false);
			return;
		}
		if(state.in_column_tag && state.column_tags.back() == name) {
			// Closing a column tag
			state.in_column_tag = false;
			return;
		}
	});

	XML_SetCharacterDataHandler(parser, [](void *user_data, const XML_Char *s, int len) {
		auto &state = *reinterpret_cast<SniffState *>(user_data);
		if(state.in_column_tag) {
			// We are in a column tag, append the text
			state.column_text.back().append(s, len);
		}
	});

	char buffer[4096];
	auto total_bytes_available = handle.GetFileSize();
	auto total_bytes_read = 0;

	while(total_bytes_read != total_bytes_available) {
		auto bytes_read = handle.Read(buffer, sizeof(buffer));
		total_bytes_read += bytes_read;
		auto status = XML_Parse(parser, buffer, bytes_read, total_bytes_read == total_bytes_available);
		if(status == XML_STATUS_ERROR) {
			auto error_code = XML_GetErrorCode(parser);
			if(error_code == XML_ERROR_ABORTED) {
				// aborted, we are done
				break;
			} else {
				// some other error, throw an exception
				auto error_msg = string(XML_ErrorString(error_code));
				XML_ParserFree(parser);
				throw InvalidInputException("Error parsing XML file: %s", error_msg);
			}
		}
	}

	XML_ParserFree(parser);

	// Now, inspect the state to figure out what we are dealing with
	XMLSniffResult result;

	// Do we even have any column tags within the row tag?
	if(state.column_tags.empty()) {
		// No: then assume the attributes of the row tag are the columns
		result.table_layout = XMLTableLayout::ROWS;
		for(auto &kv : state.row_attributes) {
			result.column_names.push_back(kv.first);
			result.column_types.push_back(LogicalType::VARCHAR);
		}
		return result;
	}

	// Are all column tags the same?
	bool all_column_tags_same = true;
	for(size_t i = 1; i < state.column_tags.size(); i++) {
		if(state.column_tags[i] != state.column_tags[0]) {
			// nope, not all column tags are the same
			all_column_tags_same = false;
		}
	}
	if(all_column_tags_same) {
		// Yes: then check that they all have a "name" attribute
		bool all_has_name_attribute = true;
		for (size_t i = 0; i < state.column_tags.size(); i++) {
			auto &attributes = state.column_attributes[i];
			auto name = attributes.find("name");
			if (name == attributes.end()) {
				// nope, not all column tags have a name attribute
				all_has_name_attribute = false;
			} else {
				result.column_names.push_back(name->second);
				result.column_types.push_back(LogicalType::VARCHAR);
			}
		}
		if (all_has_name_attribute) {
			result.table_layout = XMLTableLayout::FIELDS;
			return result;
		} else {
			result.column_types.clear();
			result.column_names.clear();
		}
	}

	// Else, assume the tag is the column, and the text is the value
	result.table_layout = XMLTableLayout::COLUMNS;
	for(size_t i = 0; i < state.column_tags.size(); i++) {
		result.column_names.push_back(state.column_tags[i]);
		result.column_types.push_back(LogicalType::VARCHAR);
	}
	return result;
}


//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct XMLBindInfo : public TableFunctionData {
	// The name of the file to read
	string file_name;
	// Row Tag
	string row_tag;
	// The table layout we are dealing with
	XMLTableLayout table_layout;
	// The number of columns in the table
	idx_t column_count;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XMLBindInfo>();

	// first parameter is the file name
	result->file_name = input.inputs[0].GetValue<string>();
	result->row_tag = "row";
	for (auto &kv : input.named_parameters) {
		auto &name = kv.first;
		auto &value = kv.second;
		if (name == "row_tag") {
			result->row_tag = value.GetValue<string>();
		}
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(result->file_name, FileFlags::FILE_FLAGS_READ);

	// Sniff the file to figure out the format
	auto sniff_result = SniffXLSX(*handle, result->row_tag);
	result->table_layout = sniff_result.table_layout;
	result->column_count = sniff_result.column_names.size();

	return_types = sniff_result.column_types;
	names = sniff_result.column_names;

	return std::move(result);
}



//------------------------------------------------------------------------------
// Different table layout parsers
//------------------------------------------------------------------------------
class FileTableParser : public XMLStateMachine<FileTableParser> {

};


class ColumnsAsRowAttributesParser : public XMLStateMachine<ColumnsAsRowAttributesParser> {
	string row_tag;
	bool in_row_tag = false;
	DataChunk payload_chunk;
	idx_t written_rows = 0;
public:

	explicit ColumnsAsRowAttributesParser(ClientContext &ctx, idx_t column_count_p, const string &row_tag_p)
	    : row_tag(row_tag_p) {
		payload_chunk.Initialize(ctx, vector<LogicalType>(column_count_p, LogicalType::VARCHAR));
	}

	void OnStartElement(const char *name, const char **atts) {
		if(!in_row_tag && strcmp(name, row_tag.c_str()) == 0) {
			// Opening row tag
			in_row_tag = true;

			// Read attributes
			auto column_count = payload_chunk.ColumnCount();
			auto attribute_count = 0;
			for(size_t i = 0; atts[i]; i += 2) {
				attribute_count++;
			}

			for(idx_t column_idx = 0; column_idx < column_count; column_idx++) {
				auto &payload_column = payload_chunk.data[column_idx];
				if(column_idx >= attribute_count) {
					FlatVector::SetNull(payload_column, written_rows, true);
				} else {
					auto &value = atts[column_idx * 2 + 1];
					auto str = StringVector::AddStringOrBlob(payload_column, value, strlen(value));
					FlatVector::GetData<string_t>(payload_column)[written_rows] = str;
				}
			}
		}
	}

	void OnEndElement(const char *name) {
		if(in_row_tag && strcmp(name, row_tag.c_str()) == 0) {
			// Closing row tag
			in_row_tag = false;
			written_rows++;

			if(written_rows == STANDARD_VECTOR_SIZE) {
				// Chunk is full, suspend the parser
				Suspend();
			}
		}
	}

	void OnCharacterData(const char *s, int len) {

	}
};

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

struct XMLReaderGlobalState : public GlobalTableFunctionState {
	unique_ptr<ColumnsAsRowAttributesParser> parser;
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_info = input.bind_data->Cast<XMLBindInfo>();
	auto result = make_uniq<XMLReaderGlobalState>();

	switch(bind_info.table_layout) {
	case XMLTableLayout::ROWS:
		result->parser = make_uniq<ColumnsAsRowAttributesParser>(context, bind_info.column_count, bind_info.row_tag);
		break;
	case XMLTableLayout::COLUMNS:
		break; // TODO
	case XMLTableLayout::FIELDS:
		break; // TODO
	}

	return std::move(result);
}


//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_info = data_p.bind_data->Cast<XMLBindInfo>();
	auto &global_state = data_p.global_state->Cast<XMLReaderGlobalState>();

	// TODO:
	if(!global_state.parser) {
		output.SetCardinality(0);
	}



}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------
static unique_ptr<TableRef> ReplacementScan(ClientContext &, const string &table_name, ReplacementScanData *) {
	auto lower_name = StringUtil::Lower(table_name);
	if (StringUtil::EndsWith(lower_name, ".xml")) {
		auto table_function = make_uniq<TableFunctionRef>();
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
		table_function->function = make_uniq<FunctionExpression>("read_xml", std::move(children));
		return std::move(table_function);
	}
	// else not something we can replace
	return nullptr;
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void XMLReader::Register(DatabaseInstance &db) {

	TableFunction xlsx_reader("read_xml", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal);
	xlsx_reader.named_parameters["row_tag"] = LogicalType::VARCHAR;

	ExtensionUtil::RegisterFunction(db, xlsx_reader);
	db.config.replacement_scans.emplace_back(ReplacementScan);
}


}
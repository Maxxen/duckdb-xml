#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "xlsx_reader.hpp"
#include "zip_writer.hpp"

namespace duckdb {

//-------------------------------------------------------------------------
// XLSX Helpers
//-------------------------------------------------------------------------
string ToExcelRef(idx_t row, idx_t col) {
	string result;
	do {
		result += 'A' + (col % 26);
		col /= 26;
	} while (col > 0);
	std::reverse(result.begin(), result.end());
	result += std::to_string(row + 1);
	return result;
}

class StringTable {
private:
	idx_t count = 0;
	unordered_map<string, idx_t> string_map;
	vector<const_reference<string>> string_list;

public:
	idx_t GetStringId(const string &str) {
		count++;
		auto entry = string_map.find(str);
		if (entry == string_map.end()) {
			auto new_id = string_list.size();

			// Maps do not invalidate references to key/value pairs even on rehashing
			auto &str_ref = string_map.insert(make_pair(str, new_id)).first->first;
			// We store references to the strings in a vector so that we can write them out in insertion order
			string_list.push_back(str_ref);

			return new_id;
		} else {
			return entry->second;
		}
	}

	idx_t Count() const {
		return count;
	}

	idx_t UniqueCount() const {
		return string_map.size();
	}

	template <class F>
	void WriteToStream(F &&f) {
		string buf = "<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\"" +
		             std::to_string(Count()) + "\" uniqueCount=\"" + std::to_string(UniqueCount()) + "\">";
		f(buf.c_str(), buf.size());
		for (auto &entry : string_list) {
			buf.clear();
			buf.append("<si><t>" + entry.get() + "</t></si>");
			f(buf.c_str(), buf.size());
		}
		buf.clear();
		buf.append("</sst>");
		f(buf.c_str(), buf.size());
	}
};

struct SheetWriter {

	idx_t current_row = 0;

	// The string table is shared between all sheets
	StringTable &string_table;

	explicit SheetWriter(StringTable &table_p) : string_table(table_p) {
	}

	template <class F>
	void WriteHeader(F &&f, const optional_ptr<vector<string>> column_names) {
		string header = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
		header.append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
		              "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">");
		header.append("<sheetData>");

		if (column_names) {
			// Always write column names as inline strings so we dont have to deal with the string table
			// during binding when reading duckdb generated excel files.
			auto &names = *column_names;
			header.append("<row r=\"1\">");
			for (idx_t i = 0; i < names.size(); i++) {
				header.append("<c r=\"" + ToExcelRef(0, i) + "\" t=\"inlineStr\"><is><t>");
				header.append(XmlEscape(names[i]));
				header.append("</t></is></c>");
			}
			header.append("</row>");
			current_row++;
		}

		f(header.c_str(), header.size());
	}

	template <class F>
	void WriteFooter(F &&f) {
		string footer = "</sheetData>";
		footer.append("</worksheet>");
		f(footer.c_str(), footer.size());
	}

	static string XmlEscape(const string_t &input) {
		string result;
		result.reserve(input.GetSize());
		// Note that the code point U+0000, assigned to the null control character,
		// is the only character encoded in Unicode and ISO/IEC 10646 that is always
		// invalid in any XML 1.0 and 1.1 document.
		// Our only option is to escape it.
		auto ptr = input.GetData();
		for (idx_t i = 0; i < input.GetSize(); i++) {
			auto c = ptr[i];
			switch (c) {
			case '&':
				result.append("&amp;");
				break;
			case '<':
				result.append("&lt;");
				break;
			case '>':
				result.append("&gt;");
				break;
			case '"':
				result.append("&quot;");
				break;
			case '\'':
				result.append("&apos;");
				break;
			case '\0':
				result.append("\\0");
				break;
			default:
				result.append(1, c);
				break;
			}
		}
		return result;
	}

	template <class F>
	void WriteToStream(DataChunk &chunk, const vector<LogicalType> &types, F &&f) {
		string row;
		for (idx_t i = 0; i < chunk.size(); i++) {
			row.clear();
			row.append("<row r=\"" + std::to_string(current_row + 1) + "\">");
			for (idx_t j = 0; j < chunk.data.size(); j++) {
				auto ref = ToExcelRef(current_row, j);
				if (FlatVector::Validity(chunk.data[j]).RowIsValid(i)) {
					auto &type = types[j];
					auto entry = FlatVector::GetData<string_t>(chunk.data[j])[i];
					if (type.IsNumeric()) {
						row.append("<c r=\"" + ref + "\"><v>");
						row.append(entry.GetData(), entry.GetSize());
						row.append("</v></c>");
					} else if (type == LogicalType::BOOLEAN) {
						row.append("<c r=\"" + ref + "\" t=\"b\"><v>");
						// TODO: inefficient
						row.append(entry == "true" ? "1" : "0");
						row.append("</v></c>");
					} else if (type == LogicalType::DATE || type == LogicalType::TIMESTAMP ||
					           type == LogicalType::TIMESTAMP_TZ) {
						row.append("<c r=\"" + ref + "\" t=\"d\"><v>");
						row.append(entry.GetData(), entry.GetSize());
						row.append("</v></c>");
					} else {
						row.append("<c r=\"" + ref + "\" t=\"s\"><v>");
						auto id = string_table.GetStringId(XmlEscape(entry));
						row.append(std::to_string(id));
						row.append("</v></c>");
					}
				} else {
					row.append("<c r=\"" + ref + "\"></c>");
				}
			}
			row.append("</row>");
			f(row.c_str(), row.size());
			current_row++;
		}
	}
};

//-------------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------------
struct CopyXLSXBindData : public FunctionData {

	string sheet_name;
	bool write_header;
	vector<string> column_names;
	vector<LogicalType> column_types;

	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<CopyXLSXBindData>();
		res->sheet_name = sheet_name;
		res->write_header = write_header;
		res->column_names = column_names;
		res->column_types = column_types;
		return std::move(res);
	}
	bool Equals(const FunctionData &other) const override {
		auto &other_data = other.Cast<CopyXLSXBindData>();
		return sheet_name == other_data.sheet_name && write_header == other_data.write_header &&
		       column_names == other_data.column_names && column_types == other_data.column_types;
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &context, const CopyInfo &info, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {
	auto result = make_uniq<CopyXLSXBindData>();

	result->column_names = names;
	result->column_types = sql_types;

	result->sheet_name = "Sheet1";
	for (const auto &opt : info.options) {
		if (StringUtil::CIEquals(opt.first, "sheet")) {
			if (opt.second.size() != 1) {
				throw BinderException("Expected exactly one sheet name");
			}
			if (opt.second.back().type() != LogicalType::VARCHAR) {
				throw BinderException("Expected sheet name to be a string");
			}
			result->sheet_name = opt.second.back().GetValue<string>();
		} else if (StringUtil::CIEquals(opt.first, "header")) {
			if (opt.second.size() != 1) {
				throw BinderException("Expected exactly one header option");
			}
			auto &value = opt.second.back();
			if (value.type() == LogicalType::FLOAT || value.type() == LogicalType::DOUBLE ||
			    value.type().id() == LogicalTypeId::DECIMAL) {
				throw BinderException("Expected header option to be a boolean");
			}
			result->write_header = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
		} else {
			throw BinderException("Unknown option for XLSX copy: " + opt.first);
		}
	}

	return std::move(result);
}

//-------------------------------------------------------------------------
// Global Data & Init
//-------------------------------------------------------------------------
struct CopyXLSXGlobalData : public GlobalFunctionData {
	string file_path;
	StringTable string_table;
	SheetWriter sheet;
	unique_ptr<ZipFileWriter> zip;
	DataChunk chunk;

	CopyXLSXGlobalData() : string_table(), sheet(string_table) {
	}
};

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                 const string &file_path) {
	auto &bind_data = bind_data_p.Cast<CopyXLSXBindData>();
	auto result = make_uniq<CopyXLSXGlobalData>();

	result->chunk.Initialize(context, vector<LogicalType>(bind_data.column_names.size(), LogicalType::VARCHAR));
	result->file_path = file_path;
	result->zip = make_uniq<ZipFileWriter>(context, file_path);

	auto &zip = *result->zip;
	// Write the rels file
	zip.AddDirectory("_rels/");
	zip.AddFile("_rels/.rels", [&]() {
		zip.Write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		zip.Write("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
		zip.Write("<Relationship Id=\"rId1\" "
		          "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" "
		          "Target=\"xl/workbook.xml\"/>");
		zip.Write("</Relationships>");
	});

	// Add the content types
	zip.AddFile("[Content_Types].xml", [&]() {
		zip.Write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		zip.Write("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">");
		zip.Write("<Default Extension=\"xml\" ContentType=\"application/xml\"/>");
		zip.Write(
		    "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>");
		zip.Write("<Default Extension=\"xlsx\" "
		          "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\"/>");
		zip.Write("<Override PartName=\"/xl/workbook.xml\" "
		          "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>");
		zip.Write("<Override PartName=\"/xl/sharedStrings.xml\" "
		          "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>");
		zip.Write("<Override PartName=\"/xl/worksheets/sheet1.xml\" "
		          "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>");
		zip.Write("</Types>");
	});

	zip.AddDirectory("xl/");
	// Write the workbook file
	zip.AddFile("xl/workbook.xml", [&]() {
		zip.Write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		zip.Write("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
		          "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">");
		zip.Write("<sheets>");
		zip.Write(StringUtil::Format("<sheet state=\"visible\" name=\"%s\" sheetId=\"1\" r:id=\"rId1\"/>",
		                             bind_data.sheet_name));
		zip.Write("</sheets>");
		zip.Write("</workbook>");
	});

	zip.AddDirectory("xl/_rels/");
	zip.AddFile("xl/_rels/workbook.xml.rels", [&]() {
		zip.Write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		zip.Write("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
		zip.Write("<Relationship Id=\"rId1\" "
		          "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
		          "Target=\"worksheets/sheet1.xml\"/>");
		zip.Write("<Relationship Id=\"rId2\" "
		          "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" "
		          "Target=\"sharedStrings.xml\"/>");
		zip.Write("</Relationships>");
	});

	zip.AddDirectory("xl/worksheets/");
	zip.AddDirectory("xl/worksheets/_rels/");
	zip.AddFile("xl/worksheets/_rels/sheet1.xml.rels", [&]() {
		zip.Write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		zip.Write("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
		zip.Write("</Relationships>");
	});

	// Begin writing sheet
	zip.BeginFile("xl/worksheets/sheet1.xml");
	auto header_ptr = bind_data.write_header ? &bind_data.column_names : nullptr;
	result->sheet.WriteHeader([&](const char *data, idx_t size) { zip.Write(data, size); }, header_ptr);

	return std::move(result);
}

//-------------------------------------------------------------------------
// Local Data & Init
//-------------------------------------------------------------------------
static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<LocalFunctionData>();
}

//-------------------------------------------------------------------------
// Sink
//-------------------------------------------------------------------------
static void Sink(ExecutionContext &context, FunctionData &bdata, GlobalFunctionData &gstate, LocalFunctionData &lstate,
                 DataChunk &input) {
	auto &state = gstate.Cast<CopyXLSXGlobalData>();
	auto &bind_data = bdata.Cast<CopyXLSXBindData>();

	state.chunk.Reset();
	// Cast all columns to VARCHAR into the intermediate chunk
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		VectorOperations::Cast(context.client, input.data[i], state.chunk.data[i], input.size());
	}
	state.chunk.SetCardinality(input.size());
	state.chunk.Flatten();

	// Pass the intermediate chunk to the sheet writer
	state.sheet.WriteToStream(state.chunk, bind_data.column_types,
	                          [&](const char *data, idx_t size) { state.zip->Write(data, size); });
}

//-------------------------------------------------------------------------
// Combine
//-------------------------------------------------------------------------
static void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
}

//-------------------------------------------------------------------------
// Finalize
//-------------------------------------------------------------------------
static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<CopyXLSXGlobalData>();

	state.sheet.WriteFooter([&](const char *data, idx_t size) { state.zip->Write(data, size); });

	// Close the sheet entry
	state.zip->EndFile();

	// Now write the string table
	state.zip->AddFile("xl/sharedStrings.xml", [&]() {
		state.string_table.WriteToStream([&](const char *data, idx_t size) { state.zip->Write(data, size); });
	});

	// Finally, flush and close the zip file
	state.zip->Finalize();
}

//-------------------------------------------------------------------------
// SupportsType
//-------------------------------------------------------------------------
static CopyTypeSupport SupportsType(const LogicalType &type) {
	// We support all types, we just cast them to VARCHAR
	return CopyTypeSupport::SUPPORTED;
}

void XLSXReader::RegisterCopy(DatabaseInstance &db) {
	CopyFunction info("XLSX");
	info.copy_to_bind = Bind;
	info.copy_to_initialize_global = InitGlobal;
	info.copy_to_initialize_local = InitLocal;
	info.copy_to_sink = Sink;
	info.copy_to_combine = Combine;
	info.copy_to_finalize = Finalize;
	info.supports_type = SupportsType;
	info.extension = "xlsx";

	ExtensionUtil::RegisterFunction(db, info);
}

} // namespace duckdb
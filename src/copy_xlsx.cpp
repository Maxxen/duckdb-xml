#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "xlsx_reader.hpp"

#include "minizip-ng/mz.h"
#include "minizip-ng/mz_os.h"
#include "minizip-ng/mz_strm.h"
#include "minizip-ng/mz_zip.h"
#include "minizip-ng/mz_zip_rw.h"

namespace duckdb {

//-------------------------------------------------------------------------
// Minizip Wrapper
//-------------------------------------------------------------------------
struct mz_stream_duckdb {
	mz_stream base;
	FileSystem *fs;
	FileHandle *handle;
};

int32_t mz_stream_duckdb_open(void *stream, const char *path, int32_t mode) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);

	if (self.handle) {
		self.handle->Close();
		self.handle->~FileHandle();
		self.handle = nullptr;
	}

	uint8_t flags = 0;
	if (mode & MZ_OPEN_MODE_READ) {
		flags |= FileFlags::FILE_FLAGS_READ;
	}
	if (mode & MZ_OPEN_MODE_WRITE) {
		flags |= FileFlags::FILE_FLAGS_WRITE;
	}
	if (mode & MZ_OPEN_MODE_READWRITE) {
		flags |= FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ;
	}
	if (mode & MZ_OPEN_MODE_APPEND) {
		flags |= FileFlags::FILE_FLAGS_APPEND;
	}
	if (mode & MZ_OPEN_MODE_CREATE) {
		flags |= FileFlags::FILE_FLAGS_FILE_CREATE;
	}
	if (mode & MZ_OPEN_MODE_EXISTING) {
		flags |= FileFlags::FILE_FLAGS_READ;
	}
	try {
		auto file = self.fs->OpenFile(path, flags);
		if (!file) {
			return MZ_OPEN_ERROR;
		}
		self.handle = file.release();
	} catch (...) {
		return MZ_OPEN_ERROR;
	}
	return MZ_OK;
}

int32_t mz_stream_duckdb_is_open(void *stream) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	if (!self.handle) {
		return MZ_OPEN_ERROR;
	}
	return MZ_OK;
}

int32_t mz_stream_duckdb_read(void *stream, void *buf, int32_t size) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	auto bytes_read = self.handle->Read(buf, size);
	return bytes_read;
}

int32_t mz_stream_duckdb_write(void *stream, const void *buf, int32_t size) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	auto bytes_written = self.handle->Write(const_cast<void *>(buf), size);
	return bytes_written;
}

int64_t mz_stream_duckdb_tell(void *stream) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	return self.handle->SeekPosition();
}

int32_t mz_stream_duckdb_seek(void *stream, int64_t offset, int32_t origin) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	switch (origin) {
	case MZ_SEEK_SET:
		self.handle->Seek(offset);
		break;
	case MZ_SEEK_CUR:
		self.handle->Seek(self.handle->SeekPosition() + offset);
		break;
	case MZ_SEEK_END:
		// is the offset negative when seeking from the end?
		self.handle->Seek(self.handle->SeekPosition() + offset);
		break;
	default:
		return MZ_SEEK_ERROR;
	}
	return MZ_OK;
}

int32_t mz_stream_duckdb_close(void *stream) {
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	self.handle->Sync();
	self.handle->Close();
	return MZ_OK;
}

int32_t mz_stream_duckdb_error(void *stream) {
	// TODO: DuckDB's fs tend to throw on errors, so this is not needed
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(stream);
	return mz_stream_error(&self.base);
}

void *mz_stream_duckdb_create(void);

void mz_stream_duckdb_delete(void **stream) {
	if (!stream) {
		return;
	}
	auto &self = *reinterpret_cast<mz_stream_duckdb *>(*stream);
	if (self.handle) {
		self.handle->Sync();
		self.handle->Close();
		self.handle->~FileHandle();
		self.handle = nullptr;
	}
	delete reinterpret_cast<mz_stream_duckdb *>(*stream);
	*stream = nullptr;
}

static mz_stream_vtbl mz_duckdb_file_stream = {
    mz_stream_duckdb_open,
    mz_stream_duckdb_is_open,
    mz_stream_duckdb_read,
    mz_stream_duckdb_write,
    mz_stream_duckdb_tell,
    mz_stream_duckdb_seek,
    mz_stream_duckdb_close,
    mz_stream_duckdb_error,
    mz_stream_duckdb_create,
    mz_stream_duckdb_delete,
    nullptr,
    nullptr,
};

void *mz_stream_duckdb_create(void) {
	auto stream = new mz_stream_duckdb();
	stream->base.vtbl = &mz_duckdb_file_stream;
	return stream;
}

class ZipFileWriter {
private:
	void *handle;
	void *stream;

public:
	ZipFileWriter(ClientContext &context, const string &path) {
		handle = mz_zip_writer_create();
		stream = mz_stream_duckdb_create();
		auto &fs = FileSystem::GetFileSystem(context);

		auto &duckdb_stream = *reinterpret_cast<mz_stream_duckdb *>(stream);
		duckdb_stream.fs = &fs;
		duckdb_stream.handle = nullptr;

		if (mz_stream_open(stream, path.c_str(), MZ_OPEN_MODE_CREATE | MZ_OPEN_MODE_WRITE) != MZ_OK) {
			throw IOException("Failed to open file for writing");
		}

		if (mz_zip_writer_open(handle, stream, 0) != MZ_OK) {
			throw IOException("Failed to open zip for writing");
		}
	}

	~ZipFileWriter() {
		if (handle) {
			if (mz_zip_writer_is_open(handle)) {
				mz_zip_writer_close(handle);
			}
			mz_zip_writer_delete(&handle);
		}
		if (stream) {
			if (mz_stream_is_open(stream)) {
				mz_stream_close(stream);
			}
			mz_stream_delete(&stream);
		}
	}

	void AddDirectory(const string &dir_path) {
		// Must end with a slash
		D_ASSERT(dir_path[dir_path.size() - 1] == '/');
		mz_zip_file file_info = {0};
		file_info.filename = dir_path.c_str();
		file_info.compression_method = MZ_COMPRESS_METHOD_DEFLATE;
		mz_zip_writer_add_buffer(handle, nullptr, 0, &file_info);
	}

	void BeginEntry(const string &file_path) {
		mz_zip_file file_info = {0};
		file_info.filename = file_path.c_str();
		file_info.compression_method = MZ_COMPRESS_METHOD_DEFLATE;
		if (mz_zip_writer_entry_open(handle, &file_info) != MZ_OK) {
			throw IOException("Failed to open entry for writing");
		}
	}

	idx_t AddData(const char *buffer, idx_t size) {
		auto bytes_written = mz_zip_writer_entry_write(handle, buffer, size);
		if (bytes_written < 0) {
			throw IOException("Failed to write entry");
		}
		return bytes_written;
	}

	void EndEntry() {
		if (mz_zip_writer_entry_close(handle) != MZ_OK) {
			throw IOException("Failed to close entry");
		}
	}

	void Finalize() {
		if (mz_zip_writer_is_open(handle)) {
			mz_zip_writer_close(handle);
		}
		if (mz_stream_is_open(stream)) {
			mz_stream_close(stream);
		}
	}
};

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
	idx_t next_id = 0;
	idx_t count = 0;

public:
	map<string, idx_t> string_map;

	idx_t GetStringId(const string &str) {
		count++;
		auto entry = string_map.find(str);
		if (entry == string_map.end()) {
			string_map[str] = next_id;
			return next_id++;
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
		for (auto &entry : string_map) {
			buf.clear();
			buf.append("<si><t>" + entry.first + "</t></si>");
			f(buf.c_str(), buf.size());
		}
		buf.clear();
		buf.append("</sst>");
		f(buf.c_str(), buf.size());
	}
};

struct SheetWriter {

	// always starts at 1
	idx_t current_row = 0;

	StringTable &string_table;

	explicit SheetWriter(StringTable &table_p) : string_table(table_p) {
	}

	template <class F>
	void WriteHeader(F &&f) {
		string header = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
		header.append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
		              "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">");
		header.append("<sheetData>");
	}

	template <class F>
	void WriteFooter(F &&f) {
		string footer = "</sheetData>";
		footer.append("</worksheet>");
	}

	template <class F>
	void WriteToStream(DataChunk &chunk, F &&f) {
		string row;
		for (idx_t i = 0; i < chunk.size(); i++) {
			row.clear();
			row.append("<row r=\"" + std::to_string(current_row + 1) + "\">");
			for (idx_t j = 0; j < chunk.data.size(); j++) {
				if (chunk.data[j].GetType().IsNumeric()) {
					row.append("<c r=\"" + ToExcelRef(current_row, j) + "\">");
					row.append("<v>");
					row.append(chunk.data[j].GetValue(i).ToString());
					row.append("</v>");
				} else {
					row.append("<c r=\"" + ToExcelRef(current_row, j) + "\" t=\"s\">");
					row.append("<v>");
					auto str = chunk.data[j].GetValue(i).ToString();
					auto id = string_table.GetStringId(str);
					row.append(std::to_string(id));
					row.append("</v>");
				}
				row.append("</c>");
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
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CopyXLSXBindData>();
	}
	bool Equals(const FunctionData &other) const override {
		return true;
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &context, const CopyInfo &info, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {
	auto result = make_uniq<CopyXLSXBindData>();
	return result;
}

//-------------------------------------------------------------------------
// Global Data & Init
//-------------------------------------------------------------------------
struct CopyXLSXGlobalData : public GlobalFunctionData {
	string file_path;
	StringTable string_table;
	SheetWriter writer;

	unique_ptr<ZipFileWriter> zip;

	CopyXLSXGlobalData() : string_table(), writer(string_table) {
	}
};

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data,
                                                 const string &file_path) {
	auto result = make_uniq<CopyXLSXGlobalData>();

	result->file_path = file_path;
	result->zip = make_uniq<ZipFileWriter>(context, file_path);

	result->zip->BeginEntry("sheet1.xml");
	result->writer.WriteHeader([&](const char *data, idx_t size) { result->zip->AddData(data, size); });

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

	state.writer.WriteToStream(input, [&](const char *data, idx_t size) { state.zip->AddData(data, size); });
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

	state.writer.WriteFooter([&](const char *data, idx_t size) { state.zip->AddData(data, size); });

	// Close the sheet entry
	state.zip->EndEntry();

	// Now write the string table
	state.zip->BeginEntry("sharedStrings.xml");
	state.string_table.WriteToStream([&](const char *data, idx_t size) { state.zip->AddData(data, size); });
	state.zip->EndEntry();

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
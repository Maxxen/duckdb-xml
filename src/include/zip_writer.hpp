#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ZipFileWriter {
private:
	void *handle;
	void *stream;

public:
	ZipFileWriter(ClientContext &context, const string &file_name);
	~ZipFileWriter();

	void AddDirectory(const string &dir_name);
	void BeginFile(const string &file_name);
	idx_t Write(const char *buffer, idx_t write_size);
	idx_t Write(const string &str);
	idx_t Write(const char *str);
	void EndFile();
	void Finalize();

	template <class F>
	void AddFile(const string &file_name, F &&f) {
		BeginFile(file_name);
		f();
		EndFile();
	}
};

} // namespace duckdb
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

#include "xlsx_reader.hpp"
#include "miniz.hpp"

namespace duckdb {

using namespace duckdb_miniz;

//-------------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------------
struct CopyXLSXBindData : public FunctionData {

};

static unique_ptr<FunctionData> Bind(ClientContext &context, const CopyInfo &info, const vector<string> &names, const vector<LogicalType> &sql_types) {
	return nullptr;
}


//-------------------------------------------------------------------------
// Global Data & Init
//-------------------------------------------------------------------------
struct CopyXLSXGlobalData : public GlobalFunctionData {
	string file_path;
	mz_zip_archive zip_archive;
};

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data, const string &file_path) {
	auto result = make_uniq<CopyXLSXGlobalData>();

	/*
	mz_zip_zero_struct(&result->zip_archive);
	mz_zip_writer_init_from_reader(&result->zip_archive, file_path.c_str());
	auto ok = mz_zip_reader_init_file(&result->zip_archive, file_path.c_str(), 0);
	*/

	return std::move(result);
}

//-------------------------------------------------------------------------
// Sink
//-------------------------------------------------------------------------
static void Sink(ExecutionContext &context, FunctionData &bdata, GlobalFunctionData &gstate, LocalFunctionData &lstate,
                     DataChunk &input) {

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
	info.copy_to_sink = Sink;
	info.copy_to_combine = Combine;
	info.copy_to_finalize = Finalize;
	info.supports_type = SupportsType;
	info.extension = "xlsx";

}

}
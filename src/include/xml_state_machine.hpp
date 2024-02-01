#pragma once
#include "duckdb.hpp"
#include "duckdb/common/assert.hpp"

#include "expat.h"
#include "zip_archive.hpp"

namespace duckdb {

// Wrapper around expat XML parser that uses CRTP to call the correct virtual methods
// Also allows us to store the state in the parser object itself.
// Instead of having to use a void* user_data pointer, we can just subclass this class.

enum class XMLParseResult { OK, ABORTED, SUSPENDED };

template <class IMPL>
class XMLStateMachine {
private:
	XML_Parser parser;

public:
	XMLStateMachine() : parser(XML_ParserCreate(nullptr)) {
		XML_SetUserData(parser, this);
		XML_SetElementHandler(
		    parser,
		    [](void *user_data, const XML_Char *name, const XML_Char **atts) {
			    auto this_parser = static_cast<IMPL *>(user_data);
			    this_parser->OnStartElement(name, atts);
		    },
		    [](void *user_data, const XML_Char *name) {
			    auto this_parser = static_cast<IMPL *>(user_data);
			    this_parser->OnEndElement(name);
		    });
	}

	// Feed data into the parser
	// Must be called with is_final = true at the end of the file
	XMLParseResult Parse(const char *buffer, size_t len, bool is_final) {
		auto status = XML_Parse(parser, buffer, len, is_final);
		switch (status) {
		case XML_STATUS_ERROR:
			if (XML_GetErrorCode(parser) == XML_ERROR_ABORTED) {
				return XMLParseResult::ABORTED;
			} else {
				auto line_number = XML_GetCurrentLineNumber(parser);
				auto column_number = XML_GetCurrentColumnNumber(parser);
				auto error_string = XML_ErrorString(XML_GetErrorCode(parser));
				throw IOException("XML parsing error: %d:%d, %s", line_number, column_number, error_string);
			}
		case XML_STATUS_SUSPENDED:
			return XMLParseResult::SUSPENDED;
		case XML_STATUS_OK:
			return XMLParseResult::OK;
		default:
			throw InternalException("Unknown XML parsing status");
		}
	}

	XMLParseResult Resume() {
		auto status = XML_ResumeParser(parser);
		switch (status) {
		case XML_STATUS_ERROR:
			if (XML_GetErrorCode(parser) == XML_ERROR_ABORTED) {
				return XMLParseResult::ABORTED;
			} else {
				throw IOException("XML parsing error: %s", XML_ErrorString(XML_GetErrorCode(parser)));
			}
		case XML_STATUS_SUSPENDED:
			return XMLParseResult::SUSPENDED;
		case XML_STATUS_OK:
			return XMLParseResult::OK;
		default:
			throw InternalException("Unknown XML parsing status");
		}
	}

	void ParseUntilEnd(unique_ptr<FileHandle> &file) {
		char buffer[2048];
		idx_t bytes_total = file->GetFileSize();
		idx_t bytes_read = 0;
		while (bytes_read != bytes_total) {
			idx_t bytes_to_read = std::min((idx_t)sizeof(buffer), bytes_total - bytes_read);
			auto read_bytes = file->Read(buffer, bytes_to_read);
			bytes_read += read_bytes;
			auto status = Parse(buffer, read_bytes, bytes_read == bytes_total);
			while (status == XMLParseResult::SUSPENDED) {
				// We ignore suspension here, we just continue parsing
				status = Resume(); // We need to resume parsing
			}
			if (status == XMLParseResult::ABORTED) {
				// Early abort
				return;
			}
		}
	}

	void ParseUntilEnd(unique_ptr<ZipArchiveExtractStream> &stream) {
		char buffer[2048];
		while (!stream->IsDone()) {
			auto read_size = stream->Read(buffer, sizeof(buffer));
			auto status = Parse(buffer, read_size, stream->IsDone());
			while (status == XMLParseResult::SUSPENDED) {
				status = Resume();
			}
			if (status == XMLParseResult::ABORTED) {
				return;
			}
		}
	}

	~XMLStateMachine() {
		// D_ASSERT(is_done);
		XML_ParserFree(parser);
	}

protected:
	void Stop() {
		XML_StopParser(parser, false);
	}

	void EnableTextParsing(bool enable) {
		if (enable) {
			XML_SetCharacterDataHandler(parser, [](void *user_data, const XML_Char *s, int len) {
				auto this_parser = static_cast<IMPL *>(user_data);
				this_parser->OnCharacterData(s, len);
			});
		} else {
			XML_SetCharacterDataHandler(parser, nullptr);
		}
	}

	void Suspend() {
		XML_StopParser(parser, true);
	}
};

} // namespace duckdb

// Converts crawled pages to UTF-8 with lexbor, which implements the WHATWG Encoding Standard that browsers use

#include "extract_extension.hpp"

#include <lexbor/encoding/encoding.h>
#include <lexbor/html/encoding.h>

namespace duckdb {

namespace {

void AppendUtf8(string &out, lxb_codepoint_t cp) {
	if (cp < 0x80) {
		out.push_back(char(cp));
	} else if (cp < 0x800) {
		out.push_back(char(0xC0 | (cp >> 6)));
		out.push_back(char(0x80 | (cp & 0x3F)));
	} else if (cp < 0x10000) {
		out.push_back(char(0xE0 | (cp >> 12)));
		out.push_back(char(0x80 | ((cp >> 6) & 0x3F)));
		out.push_back(char(0x80 | (cp & 0x3F)));
	} else {
		out.push_back(char(0xF0 | (cp >> 18)));
		out.push_back(char(0x80 | ((cp >> 12) & 0x3F)));
		out.push_back(char(0x80 | ((cp >> 6) & 0x3F)));
		out.push_back(char(0x80 | (cp & 0x3F)));
	}
}

// Value of `charset=` in a Content-Type header, without quotes
string ContentTypeCharset(const string &content_type) {
	auto lower = StringUtil::Lower(content_type);
	auto position = lower.find("charset=");
	if (position == string::npos) {
		return string();
	}
	position += 8;
	while (position < lower.size() && (lower[position] == '"' || lower[position] == '\'' || lower[position] == ' ')) {
		position++;
	}
	auto end = lower.find_first_of(";, \"'", position);
	return content_type.substr(position, end == string::npos ? string::npos : end - position);
}

const lxb_encoding_data_t *EncodingByLabel(const string &label) {
	if (label.empty()) {
		return nullptr;
	}
	auto encoding = lxb_encoding_data_by_pre_name(const_data_ptr_cast(label.c_str()), label.size());
	// The replacement encoding is for security sensitive labels, the page is better shown as UTF-8 in SQL
	if (!encoding || encoding->encoding == LXB_ENCODING_REPLACEMENT || encoding->encoding == LXB_ENCODING_DEFAULT) {
		return nullptr;
	}
	return encoding;
}

// The encoding of <meta charset> or <meta http-equiv="Content-Type"> in the first 1024 bytes, like browsers do
const lxb_encoding_data_t *MetaEncoding(const string &body) {
	lxb_html_encoding_t meta;
	if (lxb_html_encoding_init(&meta) != LXB_STATUS_OK) {
		return nullptr;
	}
	auto data = const_data_ptr_cast(body.data());
	const lxb_encoding_data_t *encoding = nullptr;
	if (lxb_html_encoding_determine(&meta, data, data + MinValue<idx_t>(body.size(), 1024)) == LXB_STATUS_OK &&
	    lxb_html_encoding_meta_length(&meta) > 0) {
		auto entry = lxb_html_encoding_meta_entry(&meta, 0);
		encoding = lxb_encoding_data_by_pre_name(entry->name, size_t(entry->end - entry->name));
		// A page declaring UTF-16 in <meta> is ASCII compatible, so it is actually UTF-8
		if (encoding && (encoding->encoding == LXB_ENCODING_UTF_16LE || encoding->encoding == LXB_ENCODING_UTF_16BE)) {
			encoding = lxb_encoding_data(LXB_ENCODING_UTF_8);
		}
	}
	lxb_html_encoding_destroy(&meta, false);
	return encoding;
}

// LXB_ENCODING_REPLACEMENT_BUFFER is a C compound literal which C++ doesn't have
const lxb_codepoint_t REPLACEMENT[1] = {LXB_ENCODING_REPLACEMENT_CODEPOINT};

string Decode(const lxb_encoding_data_t *encoding, const char *data, idx_t size) {
	lxb_codepoint_t buffer[4096];
	lxb_encoding_decode_t decoder;
	if (lxb_encoding_decode_init(&decoder, encoding, buffer, 4096) != LXB_STATUS_OK ||
	    lxb_encoding_decode_replace_set(&decoder, REPLACEMENT, 1) != LXB_STATUS_OK) {
		throw IOException("failed to initialize the %s decoder", (const char *)encoding->name);
	}
	string out;
	out.reserve(size + size / 2);
	auto flush = [&]() {
		auto used = lxb_encoding_decode_buf_used(&decoder);
		for (size_t i = 0; i < used; i++) {
			AppendUtf8(out, buffer[i]);
		}
		lxb_encoding_decode_buf_used_set(&decoder, 0);
	};
	auto position = const_data_ptr_cast(data);
	auto end = position + size;
	while (true) {
		auto status = encoding->decode(&decoder, &position, end);
		flush();
		if (status != LXB_STATUS_SMALL_BUFFER) {
			break;
		}
	}
	// Incomplete sequences at the end become replacement characters
	lxb_encoding_decode_finish(&decoder);
	flush();
	return out;
}

} // namespace

string ToUtf8(const string &body, const string *content_type, const string &hint, string &charset) {
	auto data = const_data_ptr_cast(body.data());
	const lxb_encoding_data_t *encoding = nullptr;
	idx_t skip = 0;
	// Byte order mark wins over everything else
	if (body.size() >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF) {
		encoding = lxb_encoding_data(LXB_ENCODING_UTF_8);
		skip = 3;
	} else if (body.size() >= 2 && data[0] == 0xFF && data[1] == 0xFE) {
		encoding = lxb_encoding_data(LXB_ENCODING_UTF_16LE);
		skip = 2;
	} else if (body.size() >= 2 && data[0] == 0xFE && data[1] == 0xFF) {
		encoding = lxb_encoding_data(LXB_ENCODING_UTF_16BE);
		skip = 2;
	}
	if (!encoding && content_type) {
		encoding = EncodingByLabel(ContentTypeCharset(*content_type));
	}
	if (!encoding) {
		encoding = MetaEncoding(body);
	}
	if (!encoding) {
		encoding = EncodingByLabel(hint);
	}
	if (!encoding) {
		encoding = lxb_encoding_data(LXB_ENCODING_UTF_8);
	}
	charset = string(const_char_ptr_cast(encoding->name));
	if (encoding->encoding == LXB_ENCODING_UTF_8) {
		// Most pages are UTF-8, validating is much faster than decoding and doesn't copy valid input
		return MakeValidUtf8(skip ? body.substr(skip) : body);
	}
	return Decode(encoding, body.data() + skip, body.size() - skip);
}

} // namespace duckdb

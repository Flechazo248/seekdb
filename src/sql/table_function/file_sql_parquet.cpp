/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/table_function/file_sql_parquet.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// ===========================================================================
// parquet.thrift 中的枚举
// ===========================================================================

// enum Type
enum ParquetPhysicalType
{
  PARQUET_TYPE_BOOLEAN = 0,
  PARQUET_TYPE_INT32 = 1,
  PARQUET_TYPE_INT64 = 2,
  PARQUET_TYPE_INT96 = 3,
  PARQUET_TYPE_FLOAT = 4,
  PARQUET_TYPE_DOUBLE = 5,
  PARQUET_TYPE_BYTE_ARRAY = 6,
  PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY = 7,
};

// enum FieldRepetitionType
enum ParquetRepetitionType
{
  PARQUET_REPETITION_REQUIRED = 0,
  PARQUET_REPETITION_OPTIONAL = 1,
  PARQUET_REPETITION_REPEATED = 2,
};

// enum Encoding
enum ParquetEncoding
{
  PARQUET_ENCODING_PLAIN = 0,
  PARQUET_ENCODING_PLAIN_DICTIONARY = 2,
  PARQUET_ENCODING_RLE = 3,
  PARQUET_ENCODING_BIT_PACKED = 4,
  PARQUET_ENCODING_DELTA_BINARY_PACKED = 5,
  PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY = 6,
  PARQUET_ENCODING_DELTA_BYTE_ARRAY = 7,
  PARQUET_ENCODING_RLE_DICTIONARY = 8,
  PARQUET_ENCODING_BYTE_STREAM_SPLIT = 9,
};

// enum CompressionCodec
enum ParquetCodec
{
  PARQUET_CODEC_UNCOMPRESSED = 0,
  PARQUET_CODEC_SNAPPY = 1,
  PARQUET_CODEC_GZIP = 2,
  PARQUET_CODEC_LZO = 3,
  PARQUET_CODEC_BROTLI = 4,
  PARQUET_CODEC_LZ4 = 5,
  PARQUET_CODEC_ZSTD = 6,
  PARQUET_CODEC_LZ4_RAW = 7,
};

// enum PageType
enum ParquetPageType
{
  PARQUET_PAGE_DATA = 0,
  PARQUET_PAGE_INDEX = 1,
  PARQUET_PAGE_DICTIONARY = 2,
  PARQUET_PAGE_DATA_V2 = 3,
};

// thrift compact protocol 的类型
enum ParquetThriftType
{
  PARQUET_TT_STOP = 0x00,
  PARQUET_TT_BOOLEAN_TRUE = 0x01,
  PARQUET_TT_BOOLEAN_FALSE = 0x02,
  PARQUET_TT_BYTE = 0x03,
  PARQUET_TT_I16 = 0x04,
  PARQUET_TT_I32 = 0x05,
  PARQUET_TT_I64 = 0x06,
  PARQUET_TT_DOUBLE = 0x07,
  PARQUET_TT_BINARY = 0x08,
  PARQUET_TT_LIST = 0x09,
  PARQUET_TT_SET = 0x0A,
  PARQUET_TT_MAP = 0x0B,
  PARQUET_TT_STRUCT = 0x0C,
};

// ===========================================================================
// Thrift compact protocol 读取器
// ===========================================================================

// 从缓冲区读取 varint（大端 7 bit 分组）
int parquet_read_varint(const char *data, int64_t length, int64_t &pos, uint64_t &value)
{
  int ret = OB_SUCCESS;
  uint64_t result = 0;
  int shift = 0;
  bool done = false;
  for (int i = 0; i < 10 && !done; ++i) {
    if (pos >= length) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    const uint8_t byte = static_cast<uint8_t>(data[pos]);
    ++pos;
    result |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if (0 == (byte & 0x80)) {
      done = true;
    } else {
      shift += 7;
    }
  }
  if (OB_SUCC(ret) && !done) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    value = result;
  }
  return ret;
}

// 读取 4 字节小端整数
uint32_t parquet_read_le32(const char *data)
{
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << (8 * i);
  }
  return value;
}

// thrift compact protocol 读取器：只实现解析与跳过，不做对象序列化
class ParquetCompactReader
{
public:
  ParquetCompactReader() : data_(nullptr), length_(0), pos_(0), last_field_id_(0), depth_(0) {}
  ParquetCompactReader(const char *data, int64_t length)
    : data_(data), length_(length), pos_(0), last_field_id_(0), depth_(0)
  {}

  int64_t pos() const { return pos_; }
  void seek(int64_t pos) { pos_ = pos; }

  int read_byte(uint8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = static_cast<uint8_t>(data_[pos_]);
      ++pos_;
    }
    return ret;
  }

  int read_varint(uint64_t &value)
  {
    return parquet_read_varint(data_, length_, pos_, value);
  }

  // zigzag 编码的有符号整数
  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
    } else {
      value = static_cast<int64_t>(raw >> 1) ^ -static_cast<int64_t>(raw & 1);
    }
    return ret;
  }

  int read_double(double &value)
  {
    int ret = OB_SUCCESS;
    if (length_ - pos_ < 8) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      uint64_t bits = 0;
      for (int i = 0; i < 8; ++i) {
        bits |= static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_ + i])) << (8 * i);
      }
      pos_ += 8;
      double tmp = 0;
      MEMCPY(&tmp, &bits, sizeof(tmp));
      value = tmp;
    }
    return ret;
  }

  // 读取二进制串，返回的指针指向底层缓冲区
  int read_binary(const char *&ptr, int64_t &len)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    if (OB_FAIL(read_varint(size))) {
    } else if (size > static_cast<uint64_t>(length_ - pos_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ptr = data_ + pos_;
      len = static_cast<int64_t>(size);
      pos_ += len;
    }
    return ret;
  }

  // 读取字段头：field id 为相对上一个字段的增量，类型在低 4 bit
  int read_field_header(int32_t &field_id, int32_t &type)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint8_t header = static_cast<uint8_t>(data_[pos_]);
      ++pos_;
      const int32_t delta = (header & 0xF0) >> 4;
      type = header & 0x0F;
      if (PARQUET_TT_STOP == type) {
        // 结构结束标记，后面没有字段 id
        field_id = 0;
      } else if (0 == delta) {
        int64_t id = 0;
        if (OB_FAIL(read_zigzag(id))) {
        } else {
          field_id = static_cast<int32_t>(id);
        }
      } else {
        field_id = last_field_id_ + delta;
      }
      if (OB_SUCC(ret) && PARQUET_TT_STOP != type) {
        last_field_id_ = field_id;
      }
    }
    return ret;
  }

  // 进入/退出嵌套结构：嵌套结构的字段 id 从 0 开始重新计数
  int push_struct()
  {
    int ret = OB_SUCCESS;
    if (depth_ >= MAX_STRUCT_DEPTH) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      field_id_stack_[depth_] = last_field_id_;
      ++depth_;
      last_field_id_ = 0;
    }
    return ret;
  }

  int pop_struct()
  {
    int ret = OB_SUCCESS;
    if (depth_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      --depth_;
      last_field_id_ = field_id_stack_[depth_];
    }
    return ret;
  }

  // 跳过任意类型的字段值
  int skip(int32_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case PARQUET_TT_BOOLEAN_TRUE:
      case PARQUET_TT_BOOLEAN_FALSE: {
        break;
      }
      case PARQUET_TT_BYTE: {
        uint8_t value = 0;
        ret = read_byte(value);
        break;
      }
      case PARQUET_TT_I16:
      case PARQUET_TT_I32:
      case PARQUET_TT_I64: {
        int64_t value = 0;
        ret = read_zigzag(value);
        break;
      }
      case PARQUET_TT_DOUBLE: {
        double value = 0;
        ret = read_double(value);
        break;
      }
      case PARQUET_TT_BINARY: {
        const char *ptr = nullptr;
        int64_t len = 0;
        ret = read_binary(ptr, len);
        break;
      }
      case PARQUET_TT_LIST:
      case PARQUET_TT_SET: {
        int64_t count = 0;
        int32_t elem_type = 0;
        if (OB_FAIL(read_list_header(count, elem_type))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            ret = skip(elem_type);
          }
        }
        break;
      }
      case PARQUET_TT_MAP: {
        uint64_t count = 0;
        if (OB_FAIL(read_varint(count))) {
        } else if (count > 0) {
          if (pos_ >= length_) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            const uint8_t types = static_cast<uint8_t>(data_[pos_]);
            ++pos_;
            const int32_t key_type = (types & 0xF0) >> 4;
            const int32_t value_type = types & 0x0F;
            for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              if (OB_FAIL(skip(key_type))) {
                break;
              }
              ret = skip(value_type);
            }
          }
        }
        break;
      }
      case PARQUET_TT_STRUCT: {
        ret = skip_struct();
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }
    return ret;
  }

  // 读取 list/set 的头部：元素个数与元素类型
  int read_list_header(int64_t &count, int32_t &elem_type)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint8_t header = static_cast<uint8_t>(data_[pos_]);
      ++pos_;
      elem_type = header & 0x0F;
      const int32_t size = (header & 0xF0) >> 4;
      if (0x0F == size) {
        uint64_t value = 0;
        if (OB_FAIL(read_varint(value))) {
        } else {
          count = static_cast<int64_t>(value);
        }
      } else {
        count = size;
      }
    }
    return ret;
  }

private:
  static const int64_t MAX_STRUCT_DEPTH = 32;

  int skip_struct()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(push_struct())) {
    } else {
      while (OB_SUCC(ret)) {
        int32_t field_id = 0;
        int32_t type = 0;
        if (OB_FAIL(read_field_header(field_id, type))) {
          break;
        }
        if (PARQUET_TT_STOP == type) {
          break;
        }
        ret = skip(type);
      }
      if (OB_FAIL(pop_struct())) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  const char *data_;
  int64_t length_;
  int64_t pos_;
  int32_t last_field_id_;
  int32_t depth_;
  int32_t field_id_stack_[MAX_STRUCT_DEPTH];
};

// 读取一个 i32 字段
int parquet_read_i32(ParquetCompactReader &reader, int32_t &value)
{
  int ret = OB_SUCCESS;
  int64_t raw = 0;
  if (OB_FAIL(reader.read_zigzag(raw))) {
  } else {
    value = static_cast<int32_t>(raw);
  }
  return ret;
}

// 读取一个 i64 字段
int parquet_read_i64(ParquetCompactReader &reader, int64_t &value)
{
  return reader.read_zigzag(value);
}

// ===========================================================================
// 文件元信息
// ===========================================================================

struct ParquetSchemaElement
{
  std::string name_;
  int32_t type_ = -1;
  int32_t type_length_ = 0;
  int32_t repetition_type_ = -1;
  int32_t num_children_ = 0;
  int32_t converted_type_ = -1;
};

struct ParquetColumnMeta
{
  int32_t type_ = -1;
  int32_t codec_ = PARQUET_CODEC_UNCOMPRESSED;
  int64_t num_values_ = 0;
  int64_t data_page_offset_ = 0;
  int64_t dictionary_page_offset_ = -1;
};

struct ParquetRowGroup
{
  std::vector<ParquetColumnMeta> columns_;
  int64_t num_rows_ = 0;
};

struct ParquetFileMeta
{
  std::vector<ParquetSchemaElement> schema_;
  std::vector<ParquetRowGroup> row_groups_;
  int64_t num_rows_ = 0;
};

// 一个叶子列（schema 树中的叶子节点）
struct ParquetLeafColumn
{
  std::string name_;
  int32_t type_ = -1;
  int32_t type_length_ = 0;
  int32_t converted_type_ = -1;
  int32_t max_def_level_ = 0;
  int32_t max_rep_level_ = 0;
  bool nullable_ = false;
};

int parquet_parse_schema_element(ParquetCompactReader &reader, ParquetSchemaElement &element)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, element.type_);
          break;
        }
        case 2: {
          ret = parquet_read_i32(reader, element.type_length_);
          break;
        }
        case 3: {
          ret = parquet_read_i32(reader, element.repetition_type_);
          break;
        }
        case 4: {
          const char *ptr = nullptr;
          int64_t len = 0;
          if (OB_FAIL(reader.read_binary(ptr, len))) {
          } else {
            element.name_.assign(ptr, len);
          }
          break;
        }
        case 5: {
          ret = parquet_read_i32(reader, element.num_children_);
          break;
        }
        case 6: {
          ret = parquet_read_i32(reader, element.converted_type_);
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_column_meta(ParquetCompactReader &reader, ParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, meta.type_);
          break;
        }
        case 4: {
          ret = parquet_read_i32(reader, meta.codec_);
          break;
        }
        case 5: {
          ret = parquet_read_i64(reader, meta.num_values_);
          break;
        }
        case 9: {
          ret = parquet_read_i64(reader, meta.data_page_offset_);
          break;
        }
        case 11: {
          ret = parquet_read_i64(reader, meta.dictionary_page_offset_);
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_column_chunk(ParquetCompactReader &reader, ParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      if (3 == field_id) {
        ret = parquet_parse_column_meta(reader, meta);
      } else {
        ret = reader.skip(type);
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_row_group(ParquetCompactReader &reader, ParquetRowGroup &row_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          int64_t count = 0;
          int32_t elem_type = 0;
          if (OB_FAIL(reader.read_list_header(count, elem_type))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ParquetColumnMeta meta;
              if (OB_FAIL(parquet_parse_column_chunk(reader, meta))) {
                break;
              }
              row_group.columns_.push_back(meta);
            }
          }
          break;
        }
        case 3: {
          ret = parquet_read_i64(reader, row_group.num_rows_);
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_file_metadata(ParquetCompactReader &reader, ParquetFileMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 2: {
          int64_t count = 0;
          int32_t elem_type = 0;
          if (OB_FAIL(reader.read_list_header(count, elem_type))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ParquetSchemaElement element;
              if (OB_FAIL(parquet_parse_schema_element(reader, element))) {
                break;
              }
              meta.schema_.push_back(element);
            }
          }
          break;
        }
        case 3: {
          ret = parquet_read_i64(reader, meta.num_rows_);
          break;
        }
        case 4: {
          int64_t count = 0;
          int32_t elem_type = 0;
          if (OB_FAIL(reader.read_list_header(count, elem_type))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ParquetRowGroup row_group;
              if (OB_FAIL(parquet_parse_row_group(reader, row_group))) {
                break;
              }
              meta.row_groups_.push_back(row_group);
            }
          }
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

// schema 是深度优先排列的树，这里递归收集叶子节点并计算定义级别
int parquet_collect_leaves(const std::vector<ParquetSchemaElement> &schema, int64_t &index,
                           int32_t def_level, int32_t rep_level,
                           std::vector<ParquetLeafColumn> &leaves)
{
  int ret = OB_SUCCESS;
  if (index >= static_cast<int64_t>(schema.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet schema is truncated", K(ret), K(index));
  } else {
    const ParquetSchemaElement &element = schema[index];
    int32_t my_def_level = def_level;
    int32_t my_rep_level = rep_level;
    if (PARQUET_REPETITION_OPTIONAL == element.repetition_type_) {
      my_def_level += 1;
    } else if (PARQUET_REPETITION_REPEATED == element.repetition_type_) {
      my_def_level += 1;
      my_rep_level += 1;
    }
    ++index;
    if (element.num_children_ > 0) {
      for (int32_t i = 0; OB_SUCC(ret) && i < element.num_children_; ++i) {
        ret = parquet_collect_leaves(schema, index, my_def_level, my_rep_level, leaves);
      }
    } else {
      ParquetLeafColumn leaf;
      leaf.name_ = element.name_;
      leaf.type_ = element.type_;
      leaf.type_length_ = element.type_length_;
      leaf.converted_type_ = element.converted_type_;
      leaf.max_def_level_ = my_def_level;
      leaf.max_rep_level_ = my_rep_level;
      leaf.nullable_ = (my_def_level > 0);
      leaves.push_back(leaf);
    }
  }
  return ret;
}

int parquet_build_leaves(const ParquetFileMeta &meta, std::vector<ParquetLeafColumn> &leaves)
{
  int ret = OB_SUCCESS;
  if (meta.schema_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet file has empty schema", K(ret));
  } else {
    // schema[0] 是根节点，根节点自身不计入定义级别
    const ParquetSchemaElement &root = meta.schema_[0];
    int64_t index = 1;
    for (int32_t i = 0; OB_SUCC(ret) && i < root.num_children_; ++i) {
      ret = parquet_collect_leaves(meta.schema_, index, 0, 0, leaves);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(leaves.size()); ++i) {
      if (leaves[i].max_rep_level_ > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("repeated parquet column is not supported", K(ret), K(leaves[i].name_.c_str()));
      }
    }
  }
  return ret;
}

// ===========================================================================
// 页头
// ===========================================================================

struct ParquetPageHeader
{
  int32_t type_ = PARQUET_PAGE_DATA;
  int32_t uncompressed_page_size_ = 0;
  int32_t compressed_page_size_ = 0;
  // data page (v1)
  int32_t num_values_ = 0;
  int32_t encoding_ = PARQUET_ENCODING_PLAIN;
  int32_t definition_level_encoding_ = PARQUET_ENCODING_RLE;
  int32_t repetition_level_encoding_ = PARQUET_ENCODING_RLE;
  // data page v2
  int32_t num_nulls_ = 0;
  int32_t num_rows_ = 0;
  int32_t definition_levels_byte_length_ = 0;
  int32_t repetition_levels_byte_length_ = 0;
  bool is_compressed_ = true;
  // dictionary page
  int32_t dict_num_values_ = 0;
  int32_t dict_encoding_ = PARQUET_ENCODING_PLAIN;
  bool is_sorted_ = false;
};

int parquet_parse_data_page_header(ParquetCompactReader &reader, ParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, header.num_values_);
          break;
        }
        case 2: {
          ret = parquet_read_i32(reader, header.encoding_);
          break;
        }
        case 3: {
          ret = parquet_read_i32(reader, header.definition_level_encoding_);
          break;
        }
        case 4: {
          ret = parquet_read_i32(reader, header.repetition_level_encoding_);
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_data_page_header_v2(ParquetCompactReader &reader, ParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, header.num_values_);
          break;
        }
        case 2: {
          ret = parquet_read_i32(reader, header.num_nulls_);
          break;
        }
        case 3: {
          ret = parquet_read_i32(reader, header.num_rows_);
          break;
        }
        case 4: {
          ret = parquet_read_i32(reader, header.encoding_);
          break;
        }
        case 5: {
          ret = parquet_read_i32(reader, header.definition_levels_byte_length_);
          break;
        }
        case 6: {
          ret = parquet_read_i32(reader, header.repetition_levels_byte_length_);
          break;
        }
        case 7: {
          if (PARQUET_TT_BOOLEAN_TRUE == type) {
            header.is_compressed_ = true;
          } else if (PARQUET_TT_BOOLEAN_FALSE == type) {
            header.is_compressed_ = false;
          } else {
            ret = reader.skip(type);
          }
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_dictionary_page_header(ParquetCompactReader &reader, ParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, header.dict_num_values_);
          break;
        }
        case 2: {
          ret = parquet_read_i32(reader, header.dict_encoding_);
          break;
        }
        case 3: {
          if (PARQUET_TT_BOOLEAN_TRUE == type) {
            header.is_sorted_ = true;
          } else if (PARQUET_TT_BOOLEAN_FALSE == type) {
            header.is_sorted_ = false;
          } else {
            ret = reader.skip(type);
          }
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int parquet_parse_page_header(ParquetCompactReader &reader, ParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
  } else {
    while (OB_SUCC(ret)) {
      int32_t field_id = 0;
      int32_t type = 0;
      if (OB_FAIL(reader.read_field_header(field_id, type))) {
        break;
      }
      if (PARQUET_TT_STOP == type) {
        break;
      }
      switch (field_id) {
        case 1: {
          ret = parquet_read_i32(reader, header.type_);
          break;
        }
        case 2: {
          ret = parquet_read_i32(reader, header.uncompressed_page_size_);
          break;
        }
        case 3: {
          ret = parquet_read_i32(reader, header.compressed_page_size_);
          break;
        }
        case 5: {
          ret = parquet_parse_data_page_header(reader, header);
          break;
        }
        case 7: {
          ret = parquet_parse_dictionary_page_header(reader, header);
          break;
        }
        case 8: {
          ret = parquet_parse_data_page_header_v2(reader, header);
          break;
        }
        default: {
          ret = reader.skip(type);
          break;
        }
      }
    }
    if (OB_FAIL(reader.pop_struct())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

// ===========================================================================
// 解压
// ===========================================================================

// snappy raw 格式解压：前置 varint 为解压后长度，随后是 literal/copy 标签
int parquet_snappy_decompress(const char *input, int64_t input_length, std::vector<char> &output)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t expected_length = 0;
  if (OB_FAIL(parquet_read_varint(input, input_length, pos, expected_length))) {
    LOG_WARN("failed to read snappy preamble", K(ret));
  } else if (expected_length > (uint64_t)1024 * 1024 * 1024) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snappy output size is too large", K(ret), K(expected_length));
  } else {
    output.assign(expected_length, '\0');
    size_t out_pos = 0;
    while (OB_SUCC(ret) && pos < input_length) {
      const uint8_t tag = static_cast<uint8_t>(input[pos]);
      ++pos;
      const uint32_t tag_type = tag & 0x03;
      if (0 == tag_type) {
        // literal
        uint64_t length = (tag >> 2) + 1;
        if (length > 60) {
          const uint32_t extra = static_cast<uint32_t>(length - 60);
          if (pos + extra > input_length) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          uint64_t value = 0;
          for (uint32_t i = 0; i < extra; ++i) {
            value |= static_cast<uint64_t>(static_cast<uint8_t>(input[pos + i])) << (8 * i);
          }
          pos += extra;
          length = value + 1;
        }
        if (pos + static_cast<int64_t>(length) > input_length
            || out_pos + length > output.size()) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        if (length > 0) {
          MEMCPY(output.data() + out_pos, input + pos, length);
        }
        pos += length;
        out_pos += length;
      } else {
        // copy
        uint64_t length = 0;
        uint64_t offset = 0;
        if (1 == tag_type) {
          length = 4 + ((tag >> 2) & 0x07);
          if (pos >= input_length) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          offset = (static_cast<uint64_t>(tag >> 5) << 8) | static_cast<uint8_t>(input[pos]);
          ++pos;
        } else if (2 == tag_type) {
          length = 1 + (tag >> 2);
          if (pos + 2 > input_length) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          offset = static_cast<uint64_t>(static_cast<uint8_t>(input[pos]))
                   | (static_cast<uint64_t>(static_cast<uint8_t>(input[pos + 1])) << 8);
          pos += 2;
        } else {
          length = 1 + (tag >> 2);
          if (pos + 4 > input_length) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          for (int i = 0; i < 4; ++i) {
            offset |= static_cast<uint64_t>(static_cast<uint8_t>(input[pos + i])) << (8 * i);
          }
          pos += 4;
        }
        if (0 == offset || offset > out_pos || out_pos + length > output.size()) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        // 拷贝允许重叠，逐字节复制
        for (uint64_t i = 0; i < length; ++i) {
          output[out_pos + i] = output[out_pos - offset + i];
        }
        out_pos += length;
      }
    }
    if (OB_SUCC(ret) && out_pos != output.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("snappy decompressed size mismatch", K(ret), K(out_pos), K(output.size()));
    }
  }
  return ret;
}

int parquet_decompress(int32_t codec, const char *input, int64_t input_length, std::vector<char> &output)
{
  int ret = OB_SUCCESS;
  switch (codec) {
    case PARQUET_CODEC_UNCOMPRESSED: {
      output.assign(input, input + input_length);
      break;
    }
    case PARQUET_CODEC_SNAPPY: {
      ret = parquet_snappy_decompress(input, input_length, output);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet compression codec", K(ret), K(codec));
      break;
    }
  }
  return ret;
}

// ===========================================================================
// RLE / bit-packed hybrid 编码
// ===========================================================================

// 解析 RLE/bit-packed hybrid 编码，读出 count 个值
int parquet_decode_rle_hybrid(const char *data, int64_t length, int64_t &pos,
                              int32_t bit_width, int64_t count,
                              std::vector<int32_t> &values)
{
  int ret = OB_SUCCESS;
  const uint64_t mask = (bit_width >= 64) ? UINT64_MAX : ((1ULL << bit_width) - 1);
  while (OB_SUCC(ret) && static_cast<int64_t>(values.size()) < count) {
    uint64_t header = 0;
    if (OB_FAIL(parquet_read_varint(data, length, pos, header))) {
      break;
    }
    if (0 == (header & 1)) {
      // RLE 游程
      const int64_t run_length = static_cast<int64_t>(header >> 1);
      const int64_t value_bytes = (bit_width + 7) / 8;
      uint64_t raw = 0;
      if (value_bytes > 0) {
        if (length - pos < value_bytes) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        for (int64_t i = 0; i < value_bytes; ++i) {
          raw |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i);
        }
        pos += value_bytes;
      }
      const int32_t value = static_cast<int32_t>(raw & mask);
      for (int64_t i = 0; OB_SUCC(ret) && i < run_length
           && static_cast<int64_t>(values.size()) < count; ++i) {
        values.push_back(value);
      }
    } else {
      // bit-packed 游程
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t value_count = groups * 8;
      const int64_t byte_count = (value_count * bit_width + 7) / 8;
      if (length - pos < byte_count) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      uint64_t buffer = 0;
      int64_t bits_left = 0;
      int64_t index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < value_count
           && static_cast<int64_t>(values.size()) < count; ++i) {
        while (bits_left < bit_width) {
          buffer |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + index])) << bits_left;
          ++index;
          bits_left += 8;
        }
        const int32_t value = static_cast<int32_t>(buffer & mask);
        buffer >>= bit_width;
        bits_left -= bit_width;
        values.push_back(value);
      }
      pos += byte_count;
    }
  }
  if (OB_SUCC(ret) && static_cast<int64_t>(values.size()) < count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rle hybrid data is truncated", K(ret), K(values.size()), K(count));
  }
  return ret;
}

// 计算存储一个级别需要的位宽
int32_t parquet_level_bit_width(int32_t max_level)
{
  int32_t bit_width = 0;
  while ((1 << bit_width) <= max_level) {
    ++bit_width;
  }
  return bit_width;
}

// ===========================================================================
// 值的文本化
// ===========================================================================

// 把 double 格式化为可以无损还原的文本
void parquet_format_double_text(double value, std::string &text)
{
  char buffer[64];
  buffer[0] = '\0';
  for (int precision = 15; precision <= 17; ++precision) {
    const int written = snprintf(buffer, sizeof(buffer), "%.*g", precision, value);
    if (written > 0 && strtod(buffer, nullptr) == value) {
      break;
    }
  }
  text.assign(buffer);
}

// 把物理类型的原始字节格式化为文本
int parquet_format_value(int32_t type, const char *ptr, int32_t length,
                         int32_t bit_index, std::string &text)
{
  int ret = OB_SUCCESS;
  char buffer[64];
  switch (type) {
    case PARQUET_TYPE_BOOLEAN: {
      const bool value = 0 != (static_cast<uint8_t>(ptr[0]) >> (bit_index & 7) & 0x01);
      text = value ? "true" : "false";
      break;
    }
    case PARQUET_TYPE_INT32: {
      uint32_t bits = 0;
      for (int i = 0; i < 4; ++i) {
        bits |= static_cast<uint32_t>(static_cast<uint8_t>(ptr[i])) << (8 * i);
      }
      const int written = snprintf(buffer, sizeof(buffer), "%d", static_cast<int32_t>(bits));
      text.assign(buffer, written > 0 ? written : 0);
      break;
    }
    case PARQUET_TYPE_INT64: {
      uint64_t bits = 0;
      for (int i = 0; i < 8; ++i) {
        bits |= static_cast<uint64_t>(static_cast<uint8_t>(ptr[i])) << (8 * i);
      }
      const int written = snprintf(buffer, sizeof(buffer), "%lld", static_cast<long long>(bits));
      text.assign(buffer, written > 0 ? written : 0);
      break;
    }
    case PARQUET_TYPE_FLOAT: {
      uint32_t bits = 0;
      for (int i = 0; i < 4; ++i) {
        bits |= static_cast<uint32_t>(static_cast<uint8_t>(ptr[i])) << (8 * i);
      }
      float value = 0;
      MEMCPY(&value, &bits, sizeof(value));
      parquet_format_double_text(static_cast<double>(value), text);
      break;
    }
    case PARQUET_TYPE_DOUBLE: {
      uint64_t bits = 0;
      for (int i = 0; i < 8; ++i) {
        bits |= static_cast<uint64_t>(static_cast<uint8_t>(ptr[i])) << (8 * i);
      }
      double value = 0;
      MEMCPY(&value, &bits, sizeof(value));
      parquet_format_double_text(value, text);
      break;
    }
    case PARQUET_TYPE_BYTE_ARRAY:
    case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
      text.assign(ptr, length);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet physical type", K(ret), K(type));
      break;
    }
  }
  return ret;
}

// ===========================================================================
// 数据页解码
// ===========================================================================

// 字典页：PLAIN 编码的值序列
struct ParquetDictionary
{
  int32_t type_ = -1;
  int32_t type_length_ = 0;
  int32_t num_values_ = 0;
  std::vector<char> data_;
  std::vector<int64_t> offsets_;
  std::vector<int32_t> lengths_;
};

int parquet_decode_dictionary_page(const char *data, int64_t length, int32_t type, int32_t type_length,
                                   int32_t num_values, ParquetDictionary &dictionary)
{
  int ret = OB_SUCCESS;
  dictionary.type_ = type;
  dictionary.type_length_ = type_length;
  dictionary.num_values_ = num_values;
  if (length > 0) {
    dictionary.data_.assign(data, data + length);
  }
  dictionary.offsets_.clear();
  dictionary.lengths_.clear();
  int64_t pos = 0;
  for (int32_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
    int64_t offset = 0;
    int32_t value_length = 0;
    switch (type) {
      case PARQUET_TYPE_BOOLEAN: {
        offset = i / 8;
        value_length = 1;
        break;
      }
      case PARQUET_TYPE_INT32:
      case PARQUET_TYPE_FLOAT: {
        offset = i * 4;
        value_length = 4;
        break;
      }
      case PARQUET_TYPE_INT64:
      case PARQUET_TYPE_DOUBLE: {
        offset = i * 8;
        value_length = 8;
        break;
      }
      case PARQUET_TYPE_BYTE_ARRAY: {
        if (length - pos < 4) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        const uint32_t size = parquet_read_le32(data + pos);
        pos += 4;
        offset = pos;
        value_length = static_cast<int32_t>(size);
        pos += size;
        break;
      }
      case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
        if (type_length <= 0) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        offset = i * type_length;
        value_length = type_length;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet dictionary type", K(ret), K(type));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (offset < 0 || value_length < 0 || offset + value_length > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      dictionary.offsets_.push_back(offset);
      dictionary.lengths_.push_back(value_length);
    }
  }
  return ret;
}

int parquet_dictionary_value(const ParquetDictionary &dictionary, int64_t index,
                             const char *&ptr, int32_t &length, int32_t &bit_index)
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= static_cast<int64_t>(dictionary.offsets_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet dictionary index is out of range", K(ret), K(index));
  } else {
    ptr = dictionary.data_.data() + dictionary.offsets_[index];
    length = dictionary.lengths_[index];
    bit_index = (PARQUET_TYPE_BOOLEAN == dictionary.type_) ? static_cast<int32_t>(index % 8) : -1;
  }
  return ret;
}

// PLAIN 编码的值序列
int parquet_decode_plain_values(const char *data, int64_t length, int32_t type, int32_t type_length,
                                int64_t count, std::vector<std::string> &values)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const char *ptr = nullptr;
    int32_t value_length = 0;
    int32_t bit_index = -1;
    switch (type) {
      case PARQUET_TYPE_BOOLEAN: {
        if (length - pos < 1) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        ptr = data + pos;
        value_length = 1;
        bit_index = static_cast<int32_t>(i % 8);
        ++pos;
        break;
      }
      case PARQUET_TYPE_INT32:
      case PARQUET_TYPE_FLOAT: {
        if (length - pos < 4) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        ptr = data + pos;
        value_length = 4;
        pos += 4;
        break;
      }
      case PARQUET_TYPE_INT64:
      case PARQUET_TYPE_DOUBLE: {
        if (length - pos < 8) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        ptr = data + pos;
        value_length = 8;
        pos += 8;
        break;
      }
      case PARQUET_TYPE_BYTE_ARRAY: {
        if (length - pos < 4) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        const uint32_t size = parquet_read_le32(data + pos);
        pos += 4;
        if (length - pos < static_cast<int64_t>(size)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        ptr = data + pos;
        value_length = static_cast<int32_t>(size);
        pos += size;
        break;
      }
      case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
        if (type_length <= 0 || length - pos < type_length) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        ptr = data + pos;
        value_length = type_length;
        pos += type_length;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet physical type", K(ret), K(type));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      std::string text;
      if (OB_FAIL(parquet_format_value(type, ptr, value_length, bit_index, text))) {
      } else {
        values.push_back(text);
      }
    }
  }
  return ret;
}

// 字典编码的值：1 字节位宽 + RLE/bit-packed hybrid 编码的下标
int parquet_decode_dictionary_values(const char *data, int64_t length, const ParquetDictionary &dictionary,
                                     int64_t count, std::vector<std::string> &values)
{
  int ret = OB_SUCCESS;
  if (length < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet dictionary encoded page is empty", K(ret));
  } else {
    const int32_t bit_width = static_cast<uint8_t>(data[0]);
    int64_t pos = 1;
    std::vector<int32_t> indices;
    if (OB_FAIL(parquet_decode_rle_hybrid(data, length, pos, bit_width, count, indices))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const char *ptr = nullptr;
        int32_t value_length = 0;
        int32_t bit_index = -1;
        if (OB_FAIL(parquet_dictionary_value(dictionary, indices[i], ptr, value_length, bit_index))) {
        } else {
          std::string text;
          if (OB_FAIL(parquet_format_value(dictionary.type_, ptr, value_length, bit_index, text))) {
          } else {
            values.push_back(text);
          }
        }
      }
    }
  }
  return ret;
}

// 解析数据页中的值
int parquet_decode_page_values(const char *data, int64_t length, int32_t encoding,
                                const ParquetLeafColumn &leaf, const ParquetDictionary &dictionary,
                                int64_t count, std::vector<std::string> &values)
{
  int ret = OB_SUCCESS;
  if (PARQUET_ENCODING_PLAIN == encoding) {
    ret = parquet_decode_plain_values(data, length, leaf.type_, leaf.type_length_, count, values);
  } else if (PARQUET_ENCODING_PLAIN_DICTIONARY == encoding
             || PARQUET_ENCODING_RLE_DICTIONARY == encoding) {
    ret = parquet_decode_dictionary_values(data, length, dictionary, count, values);
  } else if (PARQUET_ENCODING_RLE == encoding) {
    // boolean 列的 RLE 编码，值域前带 4 字节长度
    if (leaf.type_ != PARQUET_TYPE_BOOLEAN) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rle encoding is only supported for boolean column", K(ret), K(leaf.type_));
    } else if (length < 4) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const int64_t data_length = static_cast<int64_t>(parquet_read_le32(data));
      if (data_length > length - 4) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int64_t pos = 4;
        std::vector<int32_t> bits;
        if (OB_FAIL(parquet_decode_rle_hybrid(data, length, pos, 1, count, bits))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            const bool value = (0 != bits[i]);
            values.push_back(value ? "true" : "false");
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported parquet encoding", K(ret), K(encoding), K(leaf.name_.c_str()));
  }
  return ret;
}

// 解析一个数据页，产出文本值与 null 标记
int parquet_decode_data_page(const char *page_data, int64_t page_size, const ParquetPageHeader &page,
                             int32_t codec, const ParquetLeafColumn &leaf,
                             const ParquetDictionary &dictionary,
                             std::vector<std::string> &texts, std::vector<char> &nulls)
{
  int ret = OB_SUCCESS;
  std::vector<char> plain_page;
  const char *levels_data = nullptr;
  int64_t levels_length = 0;
  const char *values_data = nullptr;
  int64_t values_length = 0;

  if (PARQUET_PAGE_DATA_V2 == page.type_) {
    // v2: 重复级别、定义级别都不压缩，值可能压缩
    const int64_t rep_length = page.repetition_levels_byte_length_;
    const int64_t def_length = page.definition_levels_byte_length_;
    if (rep_length < 0 || def_length < 0 || rep_length + def_length > page_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parquet v2 page level length", K(ret), K(rep_length), K(def_length));
    } else {
      levels_data = page_data + rep_length;
      levels_length = def_length;
      const char *raw_values = page_data + rep_length + def_length;
      const int64_t raw_length = page_size - rep_length - def_length;
      if (page.is_compressed_) {
        if (OB_FAIL(parquet_decompress(codec, raw_values, raw_length, plain_page))) {
        } else {
          values_data = plain_page.data();
          values_length = static_cast<int64_t>(plain_page.size());
        }
      } else {
        values_data = raw_values;
        values_length = raw_length;
      }
    }
  } else {
    // v1: 整页压缩，解压后再解析级别段
    // 级别段形如 [4 字节长度][级别数据]，只有存在级别（max_def_level/max_rep_level > 0）时才有
    if (OB_FAIL(parquet_decompress(codec, page_data, page_size, plain_page))) {
    } else {
      const int64_t total = static_cast<int64_t>(plain_page.size());
      int64_t pos = 0;
      if (leaf.max_def_level_ > 0) {
        if (total - pos < 4) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int64_t def_length = static_cast<int64_t>(parquet_read_le32(plain_page.data() + pos));
          pos += 4;
          if (def_length < 0 || def_length > total - pos) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            levels_data = plain_page.data() + pos;
            levels_length = def_length;
            pos += def_length;
          }
        }
      }
      if (OB_SUCC(ret) && leaf.max_rep_level_ > 0) {
        if (total - pos < 4) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int64_t rep_length = static_cast<int64_t>(parquet_read_le32(plain_page.data() + pos));
          pos += 4;
          if (rep_length < 0 || rep_length > total - pos) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        values_data = plain_page.data() + pos;
        values_length = total - pos;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 解析定义级别
    std::vector<int32_t> levels;
    const int64_t num_values = page.num_values_;
    if (leaf.max_def_level_ > 0) {
      int64_t level_pos = 0;
      const int32_t bit_width = parquet_level_bit_width(leaf.max_def_level_);
      if (OB_FAIL(parquet_decode_rle_hybrid(levels_data, levels_length, level_pos,
                                            bit_width, num_values, levels))) {
      } else if (static_cast<int64_t>(levels.size()) != num_values) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      levels.assign(static_cast<size_t>(num_values), 0);
    }
    if (OB_SUCC(ret)) {
      // 统计非空值的个数
      int64_t value_count = 0;
      for (int64_t i = 0; i < num_values; ++i) {
        if (levels[i] >= leaf.max_def_level_) {
          ++value_count;
        }
      }
      std::vector<std::string> page_values;
      if (OB_FAIL(parquet_decode_page_values(values_data, values_length, page.encoding_,
                                             leaf, dictionary, value_count, page_values))) {
      } else if (static_cast<int64_t>(page_values.size()) != value_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parquet page value count mismatch", K(ret), K(page_values.size()), K(value_count));
      } else {
        int64_t value_index = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
          if (levels[i] < leaf.max_def_level_) {
            texts.push_back(std::string());
            nulls.push_back(1);
          } else {
            texts.push_back(page_values[value_index]);
            nulls.push_back(0);
            ++value_index;
          }
        }
      }
    }
  }
  return ret;
}

// 读取一个列块（一个 row group 内的一列）的全部数据
int parquet_decode_column_chunk(const char *file_data, int64_t file_size,
                                const ParquetColumnMeta &meta, const ParquetLeafColumn &leaf,
                                ParquetDictionary &dictionary,
                                std::vector<std::string> &texts, std::vector<char> &nulls)
{
  int ret = OB_SUCCESS;
  int64_t offset = (meta.dictionary_page_offset_ > 0) ? meta.dictionary_page_offset_
                                                       : meta.data_page_offset_;
  int64_t values_left = meta.num_values_;
  while (OB_SUCC(ret) && values_left > 0) {
    if (offset < 0 || offset >= file_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parquet page offset", K(ret), K(offset));
      break;
    }
    ParquetCompactReader reader(file_data, file_size);
    reader.seek(offset);
    ParquetPageHeader page;
    if (OB_FAIL(parquet_parse_page_header(reader, page))) {
      break;
    }
    const int64_t payload_offset = reader.pos();
    const int64_t payload_size = page.compressed_page_size_;
    if (payload_size < 0 || payload_offset > file_size || payload_size > file_size - payload_offset) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parquet page size", K(ret), K(payload_size));
      break;
    }
    if (PARQUET_PAGE_DICTIONARY == page.type_) {
      if (PARQUET_ENCODING_PLAIN != page.dict_encoding_
          && PARQUET_ENCODING_PLAIN_DICTIONARY != page.dict_encoding_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet dictionary encoding", K(ret), K(page.dict_encoding_));
      } else {
        std::vector<char> plain_page;
        if (OB_FAIL(parquet_decompress(meta.codec_, file_data + payload_offset, payload_size, plain_page))) {
        } else {
          ret = parquet_decode_dictionary_page(plain_page.data(), static_cast<int64_t>(plain_page.size()),
                                                leaf.type_, leaf.type_length_, page.dict_num_values_, dictionary);
        }
      }
    } else if (PARQUET_PAGE_DATA == page.type_ || PARQUET_PAGE_DATA_V2 == page.type_) {
      ret = parquet_decode_data_page(file_data + payload_offset, payload_size, page, meta.codec_,
                                     leaf, dictionary, texts, nulls);
      values_left -= page.num_values_;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet page type", K(ret), K(page.type_));
    }
    offset = payload_offset + payload_size;
  }
  return ret;
}

// ===========================================================================
// 文件读取与表结构
// ===========================================================================

// 把文本拷贝到分配器上，保证生命周期与表结构一致
int parquet_copy_text(ObIAllocator &allocator, const std::string &text, ObString &dst)
{
  int ret = OB_SUCCESS;
  char *buffer = static_cast<char *>(allocator.alloc(text.length() + 1));
  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for parquet value", K(ret), K(text.length()));
  } else {
    if (!text.empty()) {
      MEMCPY(buffer, text.c_str(), text.length());
    }
    buffer[text.length()] = '\0';
    dst.assign_ptr(buffer, static_cast<int64_t>(text.length()));
  }
  return ret;
}

// 依据 parquet schema 填充列结构
int parquet_fill_columns(const std::vector<ParquetLeafColumn> &leaves, ObIAllocator &allocator,
                         FileSqlTable &table)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(leaves.size()); ++i) {
    const ParquetLeafColumn &leaf = leaves[i];
    FileSqlColumn column;
    if (OB_FAIL(parquet_copy_text(allocator, leaf.name_, column.name_))) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    switch (leaf.type_) {
      case PARQUET_TYPE_BOOLEAN: {
        column.kind_ = FILE_SQL_VALUE_BOOL;
        column.type_.set_obj_type(ObTinyIntType);
        column.type_.set_precision(1);
        column.type_.set_scale(0);
        break;
      }
      case PARQUET_TYPE_INT32:
      case PARQUET_TYPE_INT64: {
        column.kind_ = FILE_SQL_VALUE_INT;
        column.type_.set_obj_type(ObIntType);
        column.type_.set_precision(20);
        column.type_.set_scale(0);
        break;
      }
      case PARQUET_TYPE_FLOAT:
      case PARQUET_TYPE_DOUBLE: {
        column.kind_ = FILE_SQL_VALUE_DOUBLE;
        column.type_.set_obj_type(ObDoubleType);
        break;
      }
      case PARQUET_TYPE_BYTE_ARRAY:
      case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
        column.kind_ = FILE_SQL_VALUE_VARCHAR;
        column.type_.set_obj_type(ObVarcharType);
        column.type_.set_collation_level(CS_LEVEL_IMPLICIT);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet column type", K(ret), K(leaf.type_), K(leaf.name_.c_str()));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      column.nullable_ = leaf.nullable_;
      if (OB_FAIL(table.columns_.push_back(column))) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

// 读取所有 row group 的数据，并按行组装
int parquet_fill_rows(const char *file_data, int64_t file_size, const ParquetFileMeta &meta,
                       const std::vector<ParquetLeafColumn> &leaves, ObIAllocator &allocator,
                       FileSqlTable &table)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = table.columns_.count();
  std::vector<std::vector<std::string>> column_texts(column_count);
  std::vector<std::vector<char>> column_nulls(column_count);
  for (int64_t c = 0; OB_SUCC(ret) && c < column_count; ++c) {
    ParquetDictionary dictionary;
    for (int64_t g = 0; OB_SUCC(ret) && g < static_cast<int64_t>(meta.row_groups_.size()); ++g) {
      const ParquetRowGroup &row_group = meta.row_groups_[g];
      if (c >= static_cast<int64_t>(row_group.columns_.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parquet row group has fewer columns than schema", K(ret), K(c));
        break;
      }
      ret = parquet_decode_column_chunk(file_data, file_size, row_group.columns_[c], leaves[c],
                                         dictionary, column_texts[c], column_nulls[c]);
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t r = 0; OB_SUCC(ret) && r < meta.num_rows_; ++r) {
      FileSqlRow row;
      for (int64_t c = 0; OB_SUCC(ret) && c < column_count; ++c) {
        ObString value;
        bool is_null = true;
        if (r < static_cast<int64_t>(column_texts[c].size())) {
          is_null = (0 != column_nulls[c][r]);
          if (!is_null && OB_FAIL(parquet_copy_text(allocator, column_texts[c][r], value))) {
            break;
          }
        }
        if (OB_FAIL(row.values_.push_back(value))) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_FAIL(row.nulls_.push_back(is_null))) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(table.rows_.push_back(row))) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

} // namespace

bool file_sql_is_parquet_content(const ObString &content)
{
  // parquet 文件以 PAR1 开头结尾，footer 长度位于末尾第 2 个 4 字节
  if (content.length() < 12) {
    return false;
  }
  if (0 != MEMCMP(content.ptr(), "PAR1", 4)
      || 0 != MEMCMP(content.ptr() + content.length() - 4, "PAR1", 4)) {
    return false;
  }
  const uint32_t footer_length = parquet_read_le32(content.ptr() + content.length() - 8);
  return static_cast<int64_t>(footer_length) + 8 <= content.length();
}

int file_sql_load_parquet(const ObString &content, bool sample_only,
                          ObIAllocator &allocator, FileSqlTable &table)
{
  int ret = OB_SUCCESS;
  const char *file_data = content.ptr();
  const int64_t file_size = content.length();
  ParquetFileMeta meta;
  std::vector<ParquetLeafColumn> leaves;
  if (file_size < 12) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet file is too small", K(ret), K(file_size));
  } else if (0 != MEMCMP(file_data, "PAR1", 4) || 0 != MEMCMP(file_data + file_size - 4, "PAR1", 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a parquet file: magic number mismatch", K(ret));
  } else {
    const uint32_t footer_length = parquet_read_le32(file_data + file_size - 8);
    if (static_cast<int64_t>(footer_length) + 8 > file_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parquet footer length", K(ret), K(footer_length), K(file_size));
    } else {
      ParquetCompactReader reader(file_data, file_size);
      reader.seek(file_size - 8 - static_cast<int64_t>(footer_length));
      if (OB_FAIL(parquet_parse_file_metadata(reader, meta))) {
        LOG_WARN("failed to parse parquet footer", K(ret));
      } else if (OB_FAIL(parquet_build_leaves(meta, leaves))) {
        LOG_WARN("failed to parse parquet schema", K(ret));
      } else if (leaves.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parquet file has no leaf column", K(ret));
      } else if (OB_FAIL(parquet_fill_columns(leaves, allocator, table))) {
        LOG_WARN("failed to build parquet columns", K(ret));
      } else if (!sample_only) {
        ret = parquet_fill_rows(file_data, file_size, meta, leaves, allocator, table);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to read parquet rows", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

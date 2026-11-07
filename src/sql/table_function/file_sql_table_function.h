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

#pragma once

#include "sql/table_function/table_function.h"

namespace oceanbase
{
namespace sql
{

// 文件即数据库：把本地 CSV / JSONL 文件当成表来查询。
// 列名与列类型在绑定期从文件推断，扫描期再读一次文件产出数据行。

enum FileSqlFormat
{
  FILE_SQL_FORMAT_CSV = 0,
  FILE_SQL_FORMAT_JSONL = 1,
  FILE_SQL_FORMAT_PARQUET = 2,
  FILE_SQL_FORMAT_GLOB = 3,
};

// 推断出的列类型种类
enum FileSqlValueKind
{
  FILE_SQL_VALUE_INT = 0,
  FILE_SQL_VALUE_DOUBLE = 1,
  FILE_SQL_VALUE_BOOL = 2,
  FILE_SQL_VALUE_VARCHAR = 3,
};

struct FileSqlColumn
{
  ObString name_;
  ObDataType type_;
  int kind_ = FILE_SQL_VALUE_VARCHAR;
  bool nullable_ = true;
};

// 一行数据：每列一个字符串，null_ 为 true 表示 SQL NULL
struct FileSqlRow
{
  ObArray<ObString> values_;
  ObArray<bool> nulls_;
};

struct FileSqlTable
{
  ObSEArray<FileSqlColumn, 16> columns_;
  ObArray<FileSqlRow> rows_;
};

// 解析格式名(csv/json/parquet/glob)，失败返回 OB_ERR_PARSE_ERROR
int file_sql_parse_format(const ObString &format_name, FileSqlFormat &format);

// 读取文件并推断列结构（sample_only=true 时只取列，不取数据行）
int file_sql_load(const ObString &path, FileSqlFormat format, bool sample_only,
                  common::ObIAllocator &allocator, FileSqlTable &table);

// 列出目录下的文件（glob 用），按文件名排序
int file_sql_list_files(const ObString &pattern, common::ObIAllocator &allocator,
                        ObArray<ObString> &files);

// 会话变量 enable_file_sql 未打开时返回错误
int file_sql_check_enabled(ObSQLSessionInfo *session_info);

// 把字符串复制到分配器
int file_sql_copy_to_allocator(common::ObIAllocator &allocator, const common::ObString &src,
                               common::ObString &dst);

// 创建文件表扫描
int create_file_sql_scan(const TableFunctionScanContext &context, FileSqlFormat format,
                         ITableFunctionScan *&scan);

} // namespace sql
} // namespace oceanbase

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

#include "sql/table_function/file_sql_table_function.h"

namespace oceanbase
{
namespace sql
{

// 判断文件内容是否为 parquet 文件（依据首尾的 PAR1 magic）
bool file_sql_is_parquet_content(const ObString &content);

// 解析 parquet 文件内容，填充表结构与数据行。
// 列结构取自 footer 中的 schema（每个叶子节点一列），数据行取自各 row group 的数据页。
// sample_only 为 true 时只解析列结构，不读取数据页。
// 仅支持单层（无嵌套）schema，且不支持 REPEATED 字段。
int file_sql_load_parquet(const ObString &content, bool sample_only,
                          common::ObIAllocator &allocator, FileSqlTable &table);

} // namespace sql
} // namespace oceanbase

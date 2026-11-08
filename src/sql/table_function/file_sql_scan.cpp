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
#include "sql/table_function/file_sql_table_function.h"

#include "sql/engine/expr/ob_expr_output_pack.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// 把字符串按列类型转成 ObObj
int convert_value(ObIAllocator &allocator, const ObString &value, bool is_null,
                  const FileSqlColumn &column, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (is_null) {
    obj.set_null();
  } else if (column.kind_ == FILE_SQL_VALUE_INT) {
    obj.set_int(strtoll(value.ptr(), NULL, 10));
  } else if (column.kind_ == FILE_SQL_VALUE_DOUBLE) {
    obj.set_double(strtod(value.ptr(), NULL));
  } else if (column.kind_ == FILE_SQL_VALUE_BOOL) {
    obj.set_bool(0 == value.case_compare(ObString::make_string("true"))
                 || 0 == value.case_compare(ObString::make_string("1")));
  } else if (column.kind_ == FILE_SQL_VALUE_DATE) {
    int32_t date_value = 0;
    if (OB_FAIL(file_sql_parse_date(value, date_value))) {
    } else {
      obj.set_date(date_value);
    }
  } else if (column.kind_ == FILE_SQL_VALUE_DATETIME) {
    int64_t datetime_value = 0;
    if (OB_FAIL(file_sql_parse_datetime(value, datetime_value))) {
    } else {
      obj.set_datetime(datetime_value);
    }
  } else {
    ObString copied;
    if (OB_FAIL(file_sql_copy_to_allocator(allocator, value, copied))) {
    } else {
      obj.set_string(ObVarcharType, copied);
    }
  }
  return ret;
}

} // namespace

int file_sql_copy_to_allocator(ObIAllocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(allocator.alloc(src.length() + 1));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (src.length() > 0) { MEMCPY(buf, src.ptr(), src.length()); }
    buf[src.length()] = '\0';
    dst.assign_ptr(buf, src.length());
  }
  return ret;
}

class FileSqlScan final : public ITableFunctionScan
{
public:
  explicit FileSqlScan(const TableFunctionScanContext &context)
    : context_(context), format_(FILE_SQL_FORMAT_CSV), eval_ctx_(context.exec_ctx_), row_index_(0),
      initialized_(false), closed_(false)
  {
    const ObString &options = context_.options_;
    if (0 == options.case_compare(ObString::make_string("json"))
        || 0 == options.case_compare(ObString::make_string("ndjson"))) {
      format_ = FILE_SQL_FORMAT_JSONL;
    } else if (0 == options.case_compare(ObString::make_string("glob"))) {
      format_ = FILE_SQL_FORMAT_GLOB;
    } else {
      format_ = FILE_SQL_FORMAT_CSV;
    }
    // 扫描期需要产出的列数与执行期请求的列数保持一致
    expected_columns_ = context_.column_exprs_.count();
  }
  ~FileSqlScan() override { (void) close(); }

  int open() override
  {
    int ret = OB_SUCCESS;
    ObString path;
    if (OB_FAIL(get_path(path))) {
    } else if (format_ == FILE_SQL_FORMAT_GLOB) {
      if (OB_FAIL(file_sql_list_files(path, context_.allocator_, files_))) {
      } else {
        FileSqlColumn column;
        column.name_ = ObString::make_string("file");
        column.type_.set_obj_type(ObVarcharType);
        column.type_.set_collation_level(CS_LEVEL_IMPLICIT);
        column.nullable_ = false;
        if (OB_FAIL(table_.columns_.push_back(column))) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    } else if (OB_FAIL(file_sql_load(path, format_, false, context_.allocator_, table_))) {
    } else {
      // 用绑定期推断出的列覆盖，保证列顺序与查询一致
      if (OB_SUCC(ret) && table_.columns_.count() != expected_columns_) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) { initialized_ = true; }
    return ret;
  }

  int rescan(const ObIArray<ObObj> &arguments) override
  {
    UNUSED(arguments);
    row_index_ = 0;
    return initialized_ ? OB_SUCCESS : OB_NOT_INIT;
  }

  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (!initialized_) {
      ret = OB_NOT_INIT;
    } else if (format_ == FILE_SQL_FORMAT_GLOB) {
      if (row_index_ >= files_.count()) {
        ret = OB_ITER_END;
      } else {
        cells_.reset();
        ObObj cell;
        cell.set_string(ObVarcharType, files_.at(row_index_));
        if (OB_SUCC(ret) && OB_FAIL(cells_.push_back(cell))) { ret = OB_ERR_UNEXPECTED; }
        if (OB_SUCC(ret)) {
          ++row_index_;
          row_.cells_ = cells_.get_data();
          row_.count_ = cells_.count();
          row = &row_;
        }
      }
    } else if (row_index_ >= table_.rows_.count()) {
      ret = OB_ITER_END;
    } else {
      const FileSqlRow &file_row = table_.rows_.at(row_index_);
      cells_.reset();
      {
        for (int64_t c = 0; OB_SUCC(ret) && c < table_.columns_.count(); ++c) {
          const ObString value = c < file_row.values_.count() ? file_row.values_.at(c) : ObString();
          const bool is_null = c < file_row.nulls_.count() ? file_row.nulls_.at(c) : true;
          ObObj cell;
          if (OB_SUCC(ret) && OB_FAIL(convert_value(context_.allocator_, value, is_null,
                                                   table_.columns_.at(c), cell))) {
          } else if (OB_SUCC(ret) && OB_FAIL(cells_.push_back(cell))) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ++row_index_;
        row_.cells_ = cells_.get_data();
        row_.count_ = cells_.count();
        row = &row_;
      }
    }
    return ret;
  }

  int close() override
  {
    closed_ = true;
    return OB_SUCCESS;
  }

  void set_expected_columns(int64_t count) { expected_columns_ = count; }

private:
  int get_path(ObString &path)
  {
    int ret = OB_SUCCESS;
    if (context_.argument_exprs_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObExpr *expr = context_.argument_exprs_.at(0);
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      path = datum.get_string();
    }
    return ret;
  }

  TableFunctionScanContext context_;
  FileSqlFormat format_;
  ObEvalCtx eval_ctx_;
  FileSqlTable table_;
  ObArray<ObString> files_;
  ObArray<ObObj> cells_;
  ObNewRow row_;
  int64_t row_index_;
  int64_t expected_columns_ = 0;
  bool initialized_;
  bool closed_;
};

static int compile_file_sql_format_options(const char *format_name,
                                           common::ObIAllocator &allocator,
                                           common::ObString &options)
{
  int ret = OB_SUCCESS;
  const int64_t len = static_cast<int64_t>(strlen(format_name));
  char *buf = static_cast<char *>(allocator.alloc(len + 1));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, format_name, len + 1);
    options.assign_ptr(buf, len);
  }
  return ret;
}

int compile_file_sql_csv_options(const TableItem &, common::ObIAllocator &allocator, common::ObString &options)
{ return compile_file_sql_format_options("csv", allocator, options); }

int compile_file_sql_json_options(const TableItem &, common::ObIAllocator &allocator, common::ObString &options)
{ return compile_file_sql_format_options("json", allocator, options); }

int compile_file_sql_glob_options(const TableItem &, common::ObIAllocator &allocator, common::ObString &options)
{ return compile_file_sql_format_options("glob", allocator, options); }

int compile_file_sql_parquet_options(const TableItem &, common::ObIAllocator &allocator, common::ObString &options)
{ return compile_file_sql_format_options("parquet", allocator, options); }

int create_file_sql_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan)
{
  int ret = OB_SUCCESS;
  void *memory = context.allocator_.alloc(sizeof(FileSqlScan));
  if (OB_ISNULL(memory)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    scan = new (memory) FileSqlScan(context);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

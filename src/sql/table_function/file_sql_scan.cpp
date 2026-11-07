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
                  const ObDataType &type, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (is_null) {
    obj.set_null(type.get_meta_type() == ObNullType ? ObNullType : type.get_meta_type());
  } else if (type.is_integer()) {
    int64_t number = 0;
    if (OB_SUCC(ret) && OB_FAIL(ObNumberUtil::strict_convert_to_int64(
            value, number, type.get_scale(), type.get_precision()))) {
      ret = OB_ERR_CONVERT_NUMBER_FAILED;
    } else if (OB_SUCC(ret)) {
      obj.set_int(number);
    }
  } else if (type.is_double()) {
    double number = 0.0;
    if (OB_SUCC(ret) && OB_FAIL(ObNumberUtil::strict_convert_to_double(value, number))) {
      ret = OB_ERR_CONVERT_NUMBER_FAILED;
    } else if (OB_SUCC(ret)) {
      obj.set_double(number);
    }
  } else if (type.is_bool()) {
    obj.set_bool(0 == value.case_compare(ObString::make_string("true"))
                 || 0 == value.case_compare(ObString::make_string("1")));
  } else {
    ObString copied;
    if (OB_SUCC(ret) && OB_FAIL(file_sql_copy_to_allocator(allocator, value, copied))) {
    } else if (OB_SUCC(ret)) {
      obj.set_string(copied);
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
  explicit FileSqlScan(const TableFunctionScanContext &context, FileSqlFormat format)
    : context_(context), format_(format), eval_ctx_(context.exec_ctx_), row_index_(0),
      initialized_(false), closed_(false) {}
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
      if (OB_SUCC(ret) && table_.columns_.count() != expected_columns_.count()) {
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
      } else if (OB_FAIL(cells_.reset())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObObj cell;
        cell.set_string(files_.at(row_index_));
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
      if (OB_FAIL(cells_.reset())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t c = 0; OB_SUCC(ret) && c < table_.columns_.count(); ++c) {
          const ObString value = c < file_row.values_.count() ? file_row.values_.at(c) : ObString();
          const bool is_null = c < file_row.nulls_.count() ? file_row.nulls_.at(c) : true;
          ObObj cell;
          if (OB_SUCC(ret) && OB_FAIL(convert_value(context_.allocator_, value, is_null,
                                                   table_.columns_.at(c).type_, cell))) {
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
      ObDatum datum;
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        path = datum.get_string();
      }
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

int create_file_sql_scan(const TableFunctionScanContext &context, FileSqlFormat format, ITableFunctionScan *&scan)
{
  int ret = OB_SUCCESS;
  void *memory = context.allocator_.alloc(sizeof(FileSqlScan));
  if (OB_ISNULL(memory)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    scan = new (memory) FileSqlScan(context, format);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

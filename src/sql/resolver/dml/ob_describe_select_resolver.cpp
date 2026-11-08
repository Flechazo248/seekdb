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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_describe_select_resolver.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
// 描述结果集固定六列，列名与 DuckDB 的 DESCRIBE 输出一致
const char *const DESCRIBE_COLUMN_NAME = "column_name";
const char *const DESCRIBE_COLUMN_TYPE = "column_type";
const char *const DESCRIBE_COLUMN_NULL = "null";
const char *const DESCRIBE_COLUMN_KEY = "key";
const char *const DESCRIBE_COLUMN_DEFAULT = "default";
const char *const DESCRIBE_COLUMN_EXTRA = "extra";

// key/default/extra 没有对应的元信息，统一输出文本 NULL
const char *const DESCRIBE_NULL_TEXT = "NULL";
const char *const DESCRIBE_NULLABLE_TEXT = "YES";
const char *const DESCRIBE_NOT_NULL_TEXT = "NO";

/**
 * @brief 追加一个字符串字面量
 *
 * 单引号按 SQL 标准写成两个单引号，反斜杠按默认的转义规则写成两个反斜杠。
 * 列名来自数据文件，不能直接拼进 SQL。
 */
int append_text_literal(ObSqlString &sql, const ObString &text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < text.length(); ++i) {
      const char c = text.ptr()[i];
      if (c == '\'' || c == '\\') {
        if (OB_FAIL(sql.append("\\"))) { break; }
      }
      if (OB_FAIL(sql.append(&c, 1))) { break; }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append("'"))) {
    } else {
      // do nothing
    }
  }
  return ret;
}

// 追加一个反引号引用的标识符
int append_quoted_name(ObSqlString &sql, const char *name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("`"))) {
  } else if (OB_FAIL(sql.append(name))) {
  } else if (OB_FAIL(sql.append("`"))) {
  }
  return ret;
}

// 追加一个单引号引用的固定文本
int append_quoted_text(ObSqlString &sql, const char *text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else if (OB_FAIL(sql.append(text))) {
  } else if (OB_FAIL(sql.append("'"))) {
  }
  return ret;
}

// 追加描述结果集中的一行
int append_describe_row(ObSqlString &sql, const ObString &column_name,
                         const ObString &type_name, const bool nullable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("SELECT "))) {
  } else if (OB_FAIL(append_text_literal(sql, column_name))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_NAME))) {
  } else if (OB_FAIL(sql.append(","))) {
  } else if (OB_FAIL(append_text_literal(sql, type_name))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_TYPE))) {
  } else if (OB_FAIL(sql.append(","))) {
  } else if (OB_FAIL(append_quoted_text(sql, nullable ? DESCRIBE_NULLABLE_TEXT : DESCRIBE_NOT_NULL_TEXT))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_NULL))) {
  } else if (OB_FAIL(sql.append(","))) {
  } else if (OB_FAIL(append_quoted_text(sql, DESCRIBE_NULL_TEXT))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_KEY))) {
  } else if (OB_FAIL(sql.append(","))) {
  } else if (OB_FAIL(append_quoted_text(sql, DESCRIBE_NULL_TEXT))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_DEFAULT))) {
  } else if (OB_FAIL(sql.append(","))) {
  } else if (OB_FAIL(append_quoted_text(sql, DESCRIBE_NULL_TEXT))) {
  } else if (OB_FAIL(sql.append(" AS "))) {
  } else if (OB_FAIL(append_quoted_name(sql, DESCRIBE_COLUMN_EXTRA))) {
  }
  return ret;
}
} // namespace

ObDescribeSelectResolver::ObDescribeSelectResolver(ObResolverParams &params)
    : ObSelectResolver(params)
{
}

ObDescribeSelectResolver::~ObDescribeSelectResolver()
{
}

int ObDescribeSelectResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *select_node = NULL;
  if (parse_tree.type_ == T_SELECT) {
    select_node = &parse_tree;
  } else {
    // DESCRIBE 节点里找出被描述的查询
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
      const ParseNode *child = parse_tree.children_ == NULL ? NULL : parse_tree.children_[i];
      if (child != NULL && child->type_ == T_SELECT) {
        select_node = child;
        break;
      }
    }
    if (OB_SUCC(ret) && select_node == NULL) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("DESCRIBE only supports select statement", K(ret), K(parse_tree.type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "DESCRIBE select statement");
    }
  }
  if (OB_SUCC(ret)) {
    ret = resolve_describe_select(*select_node);
  }
  return ret;
}

int ObDescribeSelectResolver::resolve_describe_select(const ParseNode &select_node)
{
  int ret = OB_SUCCESS;
  ObString describe_sql;
  // 只绑定被描述的查询，从绑定结果里取列元数据，不执行查询
  {
    ObResolverParams bind_params = params_;
    ObSelectResolver binder(bind_params);
    if (OB_FAIL(binder.resolve(*select_node))) {
      LOG_WARN("failed to resolve the described select statement", K(ret));
    } else {
      ObSelectStmt *bound_stmt = binder.get_select_stmt();
      if (OB_ISNULL(bound_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null select stmt for describe", K(ret));
      } else {
        ret = build_describe_sql(*bound_stmt, describe_sql);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to build describe sql", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // 描述结果本身是一个普通查询，作为最终语句返回
    ret = resolve_describe_sql(describe_sql);
  }
  return ret;
}

int ObDescribeSelectResolver::build_describe_sql(const ObSelectStmt &select_stmt,
                                                 ObString &describe_sql)
{
  return ob_describe_build_select_sql(*params_.allocator_, select_stmt, describe_sql);
}

int ObDescribeSelectResolver::resolve_describe_sql(const ObString &describe_sql)
{
  int ret = OB_SUCCESS;
  ParseResult select_result;
  ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
  if (OB_FAIL(parser.parse(describe_sql, select_result))) {
    LOG_WARN("failed to parse the describe result sql", K(ret));
  } else if (OB_ISNULL(select_result.result_tree_) || select_result.result_tree_->num_child_ != 1
             || OB_ISNULL(select_result.result_tree_->children_)
             || OB_ISNULL(select_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid describe result sql", K(ret));
  } else {
    ParseNode *select_stmt_node = select_result.result_tree_->children_[0];
    // 显式调用基类实现，避免再次进入 DESCRIBE 的处理
    ret = ObSelectResolver::resolve(*select_stmt_node);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to resolve the describe result sql", K(ret));
    }
  }
  return ret;
}

int ob_describe_build_select_sql(ObIAllocator &allocator,
                                  const ObSelectStmt &select_stmt,
                                  ObString &describe_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t item_count = select_stmt.get_select_item_size();
  for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
    const SelectItem &item = select_stmt.get_select_item(i);
    if (OB_ISNULL(item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select item expr for describe", K(ret), K(i));
      break;
    }
    ObString column_name = item.alias_name_.empty() ? item.expr_name_ : item.alias_name_;
    ObString type_name;
    if (OB_FAIL(ob_describe_column_type_name(item.expr_->get_result_meta(),
                                              item.expr_->get_accuracy(),
                                              allocator,
                                              type_name))) {
      LOG_WARN("failed to get the type name of the described column", K(ret), K(i));
    } else if (i > 0) {
      if (OB_FAIL(sql.append(" UNION ALL "))) {
        LOG_WARN("failed to append union all", K(ret));
      } else if (OB_FAIL(append_describe_row(sql, column_name, type_name,
                                             ob_describe_column_nullable(item)))) {
        LOG_WARN("failed to append describe row", K(ret), K(i));
      }
    } else if (OB_FAIL(append_describe_row(sql, column_name, type_name,
                                           ob_describe_column_nullable(item)))) {
      LOG_WARN("failed to append describe row", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && item_count == 0) {
    // 没有投影列时只保留表头
    ObString empty_text;
    if (OB_FAIL(append_describe_row(sql, empty_text, empty_text, true))) {
      LOG_WARN("failed to append the empty describe result", K(ret));
    } else if (OB_FAIL(sql.append(" FROM dual WHERE 1 = 0"))) {
      LOG_WARN("failed to append the empty describe result", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator, sql.string(), describe_sql);
  }
  return ret;
}

int ob_describe_column_type_name(const ObObjMeta &meta,
                                 const ObAccuracy &accuracy,
                                 ObIAllocator &allocator,
                                 ObString &type_name)
{
  int ret = OB_SUCCESS;
  const char *name = "UNKNOWN";
  switch (meta.get_type()) {
  case ObNullType:
    name = "NULL";
    break;
  case ObTinyIntType:
    // OB 用 tinyint(1) 表示布尔值，DuckDB 侧对应 BOOLEAN
    name = (1 == accuracy.get_precision()) ? "BOOLEAN" : "TINYINT";
    break;
  case ObSmallIntType:
    name = "SMALLINT";
    break;
  case ObMediumIntType:
    name = "MEDIUMINT";
    break;
  case ObInt32Type:
    name = "INT";
    break;
  case ObIntType:
    // ObIntType 是 64 位整数，对应 BIGINT
    name = "BIGINT";
    break;
  case ObUTinyIntType:
    name = "TINYINT UNSIGNED";
    break;
  case ObUSmallIntType:
    name = "SMALLINT UNSIGNED";
    break;
  case ObUMediumIntType:
    name = "MEDIUMINT UNSIGNED";
    break;
  case ObUInt32Type:
    name = "INT UNSIGNED";
    break;
  case ObUInt64Type:
    name = "BIGINT UNSIGNED";
    break;
  case ObFloatType:
    name = "FLOAT";
    break;
  case ObDoubleType:
    name = "DOUBLE";
    break;
  case ObUFloatType:
    name = "FLOAT UNSIGNED";
    break;
  case ObUDoubleType:
    name = "DOUBLE UNSIGNED";
    break;
  case ObNumberType:
    name = "DECIMAL";
    break;
  case ObUNumberType:
    name = "DECIMAL UNSIGNED";
    break;
  case ObDateTimeType:
    name = "DATETIME";
    break;
  case ObTimestampType:
    name = "TIMESTAMP";
    break;
  case ObTimestampLTZType:
    name = "TIMESTAMP";
    break;
  case ObTimestampNanoType:
    name = "TIMESTAMP";
    break;
  case ObDateType:
    name = "DATE";
    break;
  case ObTimeType:
    name = "TIME";
    break;
  case ObYearType:
    name = "YEAR";
    break;
  case ObVarcharType:
    name = "VARCHAR";
    break;
  case ObCharType:
    name = "CHAR";
    break;
  case ObHexStringType:
    name = "HEX_STRING";
    break;
  case ObExtendType:
    name = "EXT";
    break;
  case ObUnknownType:
    name = "UNKNOWN";
    break;
  case ObTinyTextType:
    name = "TINYTEXT";
    break;
  case ObTextType:
    name = "TEXT";
    break;
  case ObMediumTextType:
    name = "MEDIUMTEXT";
    break;
  case ObLongTextType:
    name = "LONGTEXT";
    break;
  case ObBitType:
    name = "BIT";
    break;
  case ObEnumType:
    name = "ENUM";
    break;
  case ObSetType:
    name = "SET";
    break;
  case ObJsonType:
    name = "JSON";
    break;
  case ObGeometryType:
    name = "GEOMETRY";
    break;
  case ObUserDefinedSQLType:
    name = "UDT";
    break;
  case ObDecimalIntType:
    name = "DECIMAL";
    break;
  case ObCollectionSQLType:
    name = "COLLECTION";
    break;
  case ObMySQLDateType:
    name = "DATE";
    break;
  case ObMySQLDateTimeType:
    name = "DATETIME";
    break;
  default:
    name = "UNKNOWN";
    break;
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator, ObString::make_string(name), type_name);
  }
  return ret;
}

bool ob_describe_column_nullable(const SelectItem &item)
{
  const ObRawExpr *expr = item.expr_;
  // 列绑定时确认非空的列会带上 NOT_NULL_FLAG，未带该标记即为可空
  return OB_ISNULL(expr) || !expr->is_not_null_for_read();
}

} // namespace sql
} // namespace oceanbase

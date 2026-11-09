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

#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief DuckDB 风格的 DESCRIBE <select>
 *
 * 被描述的查询只做绑定，不执行。绑定完成后按投影列表的顺序为每一列输出一行，
 * 结果集固定六列，与 DuckDB 的 DESCRIBE 输出一致：
 *
 *   column_name | column_type | null | key | default | extra
 *
 *   column_name  投影列名
 *   column_type  OB 类型名（大写，不带长度）
 *   null         列可空时为 YES，否则为 NO
 *   key/default/extra  固定为文本 NULL
 *
 * 例如：
 *   DESCRIBE SELECT * FROM read_csv('/tmp/basic.csv');
 *
 * 该语句的语法由语法分析层产生一个只包含被描述查询的节点，
 * 由 ObResolver 分派到本 resolver。
 */
class ObDescribeSelectResolver : public ObSelectResolver
{
public:
  explicit ObDescribeSelectResolver(ObResolverParams &params);
  virtual ~ObDescribeSelectResolver();
  /**
   * @param parse_tree 可以是 T_SELECT 本身，也可以是包含 T_SELECT 子节点的 DESCRIBE 节点
   */
  virtual int resolve(const ParseNode &parse_tree);

private:
  // 绑定被描述的查询并生成描述结果集
  int resolve_describe_select(const ParseNode &select_node);
  // 由已绑定的查询生成描述结果集的 SQL
  int build_describe_sql(const ObSelectStmt &select_stmt, common::ObString &describe_sql);
  // 把生成的 SQL 解析并绑定成最终语句
  int resolve_describe_sql(const common::ObString &describe_sql);
  DISALLOW_COPY_AND_ASSIGN(ObDescribeSelectResolver);
};

/**
 * @brief 由已绑定的查询生成 DESCRIBE 结果集的 SQL
 *
 * 结果集固定六列，每个投影列一行：
 *   column_name | column_type | null | key | default | extra
 */
int ob_describe_build_select_sql(common::ObIAllocator &allocator,
                                  const ObSelectStmt &select_stmt,
                                  common::ObString &describe_sql);

/**
 * @brief OB 类型名 -> DESCRIBE 输出的类型名
 *
 * 使用 OB 自身登记的类型名（ob_sql_type_str，见
 * src/oblib/common/object/ob_obj_type.cpp），统一大写且不带长度，
 * 与 DuckDB 的类型名习惯一致：
 *   ObIntType -> BIGINT, ObVarcharType -> VARCHAR, ObDoubleType -> DOUBLE,
 *   ObDateType -> DATE, ObDateTimeType -> DATETIME
 * OB 没有独立的布尔类型，用 tinyint(1) 表示，这里输出 BOOLEAN。
 */
int ob_describe_column_type_name(const common::ObObjMeta &meta,
                                 const common::ObAccuracy &accuracy,
                                 common::ObIAllocator &allocator,
                                 common::ObString &type_name);

/**
 * @brief 投影列是否可空
 *
 * 列绑定时若已确认非空，会在列表达式上打 NOT_NULL_FLAG，
 * 表函数列的可空信息即通过该标记传递（见 bind_file_sql_table_function）。
 */
bool ob_describe_column_nullable(const SelectItem &item);

} // namespace sql
} // namespace oceanbase

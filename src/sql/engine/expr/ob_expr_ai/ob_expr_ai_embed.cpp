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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                        T_FUN_SYS_AI_EMBED,
                        N_AI_EMBED,
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL,
                        NOT_ROW_DIMENSION)
{
}

ObExprAIEmbed::~ObExprAIEmbed()
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

// 取批量求值下第 idx 行的参数 datum: 批量参数按行取, 非批量参数(常量)取唯一值
static int get_batch_arg_datum(const ObExpr *arg_expr,
                               ObEvalCtx &ctx,
                               const int64_t idx,
                               const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg_expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (arg_expr->is_batch_result()) {
    ObDatumVector datums = arg_expr->locate_expr_datumvector(ctx);
    datum = datums.at(idx);
  } else {
    datum = &arg_expr->locate_expr_datum(ctx);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_model_id->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
    res.set_null();
  } else if (arg_content->is_null()) {
    // 输入为 null 时直接返回 null, 不发起模型请求, 也不影响其它行的结果对齐
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);

    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString content = arg_content->get_string();
    if (model_id.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOTNULL(arg_dim) && !arg_dim->is_null()) {
      dim = arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
        res.set_null();
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObExpr *model_expr = expr.args_[MODEL_IDX];
  const ObExpr *content_expr = expr.args_[CONTENT_IDX];
  const ObExpr *dim_expr = (expr.arg_cnt_ == 3) ? expr.args_[DIM_IDX] : nullptr;
  ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  // 需要发起请求的行号以及对应的输入内容
  ObArray<int64_t> target_rows;
  ObArray<ObString> contents;
  ObArray<ObString> results;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));

  if (OB_FAIL(model_expr->eval_batch(ctx, skip, size))
      || OB_FAIL(content_expr->eval_batch(ctx, skip, size))
      || (OB_NOTNULL(dim_expr) && OB_FAIL(dim_expr->eval_batch(ctx, skip, size)))) {
  } else {
    // 1. 收集需要请求的行: null 行不进入请求, 结果直接置 null
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      if (eval_flag.contain(i) || skip.contain(i)) {
        continue;
      }
      const ObDatum *content_datum = nullptr;
      if (OB_FAIL(get_batch_arg_datum(content_expr, ctx, i, content_datum))) {
      } else if (OB_ISNULL(content_datum) || content_datum->is_null()) {
        res_datums[i].set_null();
        eval_flag.set(i);
      } else {
        ObString content;
        if (ob_is_text_tc(content_expr->datum_meta_.type_)) {
          // lob 类型需要读取真实内容
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_,
                                                                 temp_allocator,
                                                                 *content_datum,
                                                                 content_expr->datum_meta_,
                                                                 content_expr->obj_meta_.has_lob_header(),
                                                                 content))) {
          }
        } else {
          content = content_datum->get_string();
        }
        if (OB_SUCC(ret) && content.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
        } else if (OB_SUCC(ret)) {
          if (OB_FAIL(contents.push_back(content)) || OB_FAIL(target_rows.push_back(i))) {
          }
        }
      }
    }

    if (OB_SUCC(ret) && contents.count() > 0) {
      // 2. 模型与端点只解析一次, 整个批次共用
      const ObDatum *model_datum = nullptr;
      int64_t dim = 0;
      ObJsonInt *dim_json = nullptr;
      ObJsonObject *config = nullptr;
      ObAIFuncExprInfo *info = nullptr;
      share::ObAiModelEndpointInfo resolved_endpoint;
      const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
      query::ObIAiEndpointResolver *endpoint_resolver =
          ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
      if (OB_FAIL(get_batch_arg_datum(model_expr, ctx, target_rows[0], model_datum))) {
      } else if (OB_ISNULL(model_datum) || model_datum->is_null()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
      } else if (OB_NOTNULL(dim_expr)) {
        const ObDatum *dim_datum = nullptr;
        if (OB_FAIL(get_batch_arg_datum(dim_expr, ctx, target_rows[0], dim_datum))) {
        } else if (OB_NOTNULL(dim_datum) && !dim_datum->is_null()) {
          dim = dim_datum->get_int();
          if (dim <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
          }
        }
      }
      ObString model_id;
      if (OB_SUCC(ret)) {
        model_id = model_datum->get_string();
        if (model_id.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
        }
      }
      if (OB_SUCC(ret) && dim > 0) {
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))
            || OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))
            || OB_FAIL(config->add("dimensions", dim_json))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
      } else if (OB_SUCC(ret) && OB_ISNULL(endpoint_resolver)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_SUCC(ret) && OB_FAIL(endpoint_resolver->resolve_by_model_name(
                            model_id, temp_allocator, resolved_endpoint))) {
      } else if (OB_SUCC(ret)) {
        // 3. 一次请求携带整批输入
        ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
        if (OB_FAIL(model.call_dense_embedding_vector_v2(contents, config, results))) {
          // 合并请求失败时退化为逐行请求, 保证结果与单行语义一致
          LOG_WARN("failed to batch embedding request, fallback to row by row", K(ret));
          ret = OB_SUCCESS;
          results.reset();
          for (int64_t k = 0; OB_SUCC(ret) && k < contents.count(); k++) {
            ObString row_result;
            if (OB_FAIL(model.call_dense_embedding(contents[k], config, row_result))) {
            } else if (OB_FAIL(results.push_back(row_result))) {
            }
          }
        }
        if (OB_SUCC(ret) && results.count() != contents.count()) {
          ret = OB_ERR_UNEXPECTED;
        }
        if (OB_SUCC(ret)) {
          // 4. 按行回填结果
          ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
          for (int64_t k = 0; OB_SUCC(ret) && k < target_rows.count(); k++) {
            batch_info_guard.set_batch_idx(target_rows[k]);
            ObString row_result = results[k];
            if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res_datums[target_rows[k]], row_result))) {
            } else {
              eval_flag.set(target_rows[k]);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOTNULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOTNULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

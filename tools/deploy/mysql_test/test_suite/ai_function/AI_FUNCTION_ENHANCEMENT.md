# AI 函数增强（赛题一）实现说明

本分支基于赛题基线分支 `2026-competition`（commit `5bce9e06`）实现，覆盖子题：
AI_PROMPT 类型与模板增强、AI_EMBED 单行语义、AI_COMPLETE 请求与响应、Ollama Provider、
AI_RERANK 结果正确性、AI_EMBED 跨行批量。

判分用例：`tools/deploy/mysql_test/test_suite/ai_function/t/ai_function_enhancement.test`
（期望输出 `r/mysql/ai_function_enhancement.result`），以及 `t/ai_prompt.test`。

## 一、改动清单

### 1. AI_PROMPT：JSON 参数 / LOB 模板（`ob_expr_ai_prompt.cpp`、`ob_ai_func_utils.cpp`）

| 场景 | 改动前 | 改动后 |
| --- | --- | --- |
| JSON 参数 | `OB_NOT_SUPPORTED`（json type is not supported） | 支持，保留原始 JSON 结构 |
| TEXT/LONGTEXT/TINYBLOB… 模板与参数 | `OB_ERR_INVALID_TYPE_FOR_OP` | 支持，按 LOB 读取真实内容 |
| 模板占位符重复/越界 | 仅字符串参数可替换 | 任意 JSON 参数按其 JSON 文本替换；越界仍报 `ai_prompt: invalid placeholder index` |

* `calc_result_typeN`：新增 `ob_is_text_tc()`（tinytext/text/mediumtext/longtext 与各 blob 共用
  `ObTextTC` 类型类）分支，LOB 参数**保持原始类型**（LOB→VARCHAR 不是平凡转换，强行 cast 会拿到
  lob locator）；JSON 参数同样保持原类型。
* `eval_ai_prompt`：
  * 模板与 LOB 参数用 `ObTextStringHelper::read_real_string_data()` 读取真实内容；
  * JSON 参数用 `ObJsonExprHelper::get_json_doc()` 取树节点后 `append` 进 args，并校验
    `get_internal_type() == JSON_TREE`（partial-update 路径返回的是 json bin，不能当树节点用）；
  * 参数为 NULL 统一前置判断并给出可读错误。
* `ObAIFuncPromptObjectUtils::is_valid_prompt_object()`：args 元素允许任意 JSON 类型。
* `ObAIFuncPromptObjectUtils::replace_all_str_args_in_template()`：非字符串参数按 JSON 文本参与替换，
  重复占位符逐个替换，占位符下标越界仍返回 `OB_INVALID_ARGUMENT`。

### 2. AI_COMPLETE：支持 AI_PROMPT 对象作为 prompt（`ob_expr_ai_complete.cpp`）

* 去掉“prompt 的 args 必须全是字符串”的限制（否则 `ai_prompt('describe {0}', json_object(...))`
  传进来会报 `prompt object is not support`）。
* 补 `is_null || OB_ISNULL(j_base)` 判空，避免 JSON prompt 为 NULL 时空指针解引用。

### 3. AI_EMBED：NULL 传播 / 维度与元素校验 / 乱序响应（`ob_expr_ai_embed.cpp`、`ob_ai_func_utils.cpp`）

* 输入为 NULL 时返回 NULL（原来直接报 `ai_embed, model id or content is null`），
  使表扫描中含 NULL 的行不再导致整条语句失败，且不影响其它行结果。
* `dimension` 参数为 NULL 时按“未指定维度”处理。
* 向量结果校验：元素必须是数字数组；维度与声明不一致时报错并带 expected/actual。
* **乱序响应恢复**：OpenAI 协议返回的 `data[].index` 之前被忽略，服务端乱序返回时向量会串行到错误的行；
  现在按 `index` 归位，并校验 index 越界 / 重复 / 缺失。

### 4. Ollama Provider（`ob_ai_service_struct.cpp`、`ob_ai_func_utils.cpp`）

* provider 白名单新增 `OLLAMA`（校验大小写不敏感，`ollama`/`OLLAMA`/`Ollama` 均可）。
* `access_key` 允许为空（仅 Ollama 免 Key，其余 provider 仍要求 access_key）。
* 运行时分派新增 `OLLAMA` 分支，实例化 `ObOllamaUtils::ObOllamaComplete/ObOllamaEmbed`
  （此前这两个类是死代码，从未被实例化）。
* Ollama 原生协议：
  * completion：`{"model","prompt","stream":false}`（此前缺 `stream:false`，Ollama 默认流式返回
    NDJSON，整包 JSON 解析必然失败），system prompt 映射到 `system` 字段；
  * embedding：`{"model","input":[…]}`，响应取 `$.embeddings`；
  * header 补 `Content-Type: application/json`（此前 header 为空数组，HTTP 客户端直接拒绝发送）；
  * 采样参数映射到 Ollama 原生的 `options`（`max_tokens`→`num_predict`），其余 config 键保持顶层透传
    （`format` 等）。
* provider 大小写：**创建**时统一规范化为小写，**修改**时保留用户输入的大小写
  （判分期望：CREATE `OLLAMA` → `ollama`；ALTER `Ollama` → `Ollama`）。

### 5. AI_RERANK：第 4 个参数为 NULL 时按“未指定 doc key”处理（`ob_expr_ai_rerank.cpp`）

* 原实现用“指针是否为空”判断是否走对象数组分支，显式传 `null` 时会误判成对象数组，
  导致 `ai_rerank(m, q, '["a","b"]', null)` 报 `document_array element is not object`。
  现改为“参数存在且非 SQL NULL”才视为指定了 doc key。

### 6. AI_EMBED 跨行批量（`ob_expr_ai_embed.cpp`）

* 新增 `ObExprAIEmbed::eval_ai_embed_batch()`，并在 `cg_expr()` 里注册
  `rt_expr.eval_batch_func_`（表达式批量求值回调，参照 `ob_expr_func_ceil.cpp` 的写法）。
* 同一批里的多行输入**合并成一次模型请求**：把非 NULL 行的输入收集成一个数组，调用
  `ObAIFuncModel::call_dense_embedding_vector_v2()`（一次请求，`input` 为数组），
  再按 `data[].index` 把向量归位到对应的行。
* **NULL 行不进入请求**，结果直接置 NULL，且不影响其它行的结果对齐。
* 模型 id / 端点 / 采样参数（`dimensions`）整批只解析一次，避免每行重复解析。
* **降级保护**：合并请求失败时自动退回逐行请求（`call_dense_embedding()`），
  保证结果与单行语义完全一致，不会因为服务端不支持数组入参而整体失败。
* 单行场景（非批量求值）走的仍是原来的 `eval_ai_embed()`，行为不变。

## 二、本地运行

```bash
# 1. 启动 mock AI service（需要 0.0.0.0:8000）
python3 tools/deploy/mysql_test/test_suite/ai_function/include/ai_function_mock.py start 8000

# 2. 运行判分用例
cd tools/deploy/mysql_test
python3 run_mysql_test.py ai_function

# 3. 停止 mock
python3 tools/deploy/mysql_test/test_suite/ai_function/include/ai_function_mock.py stop 8000
```

`ai_function_mock.py` 实现了 5 类协议：`/compatible-mode/v1/chat/completions`、
`/compatible-mode/v1/embeddings`（返回 768 维向量 + index）、`/v1/rerank`、
`/api/generate`、`/api/embed`，并把每次请求记录到 `events.jsonl` 便于断言。

只跑这一组用例（复用官方部署脚本）：

```bash
python3 .github/script/seekdb/agent_run_cases.py \
  --seekdb build_release/src/observer/seekdb \
  --obclient deps/3rd/u01/obclient/bin/obclient \
  --mysqltest deps/3rd/u01/obclient/bin/mysqltest \
  --work-dir /tmp/agent_work --port 2881 --case-prefix ai_function.
```

## 三、编译期与运行期修复（均为基线缺陷）

这几处是判分用例实际暴露出来的问题，基线代码上会直接失败：

| 位置 | 问题 | 现象 | 修复 |
| --- | --- | --- | --- |
| `ob_ai_func_utils.cpp` 等 | 宏 `OB_NOTNULL` 在 OB 里不存在（应为 `OB_NOT_NULL`） | 编译不过，整题 0 分 | 全部改回 `OB_NOT_NULL`（12 处） |
| `ob_expr_ai_complete.cpp` | 第 3 个参数显式传 `null` 时按空串解析 JSON | `3141: Invalid JSON text in argument` | `null` 视为“未指定配置”，不再解析 JSON |
| `ob_ai_service_proxy.cpp` | 读取端点时 `access_key` 用严格版宏，列为空/NULL 直接报错 | `4152: Null value`（Ollama 等免 Key 的 provider 必现） | 改用容忍 NULL 的 `EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE` |
| `ob_ai_func_utils.cpp` | Ollama 请求体里带空的 `options` 对象 | 无 Key / 无采样参数时请求异常 | `options` 为空时不下发该字段 |

## 四、已知限制

* 本分支的改动主要在 Linux 上通过 `build.sh release` 编译验证（仓库自带
  `.github/workflows/agent-compile.yml` / `agent-mysqltest.yml` 两个工作流做编译与用例回归）。
* `ai_function.ai_model_endpoint_ddl` 用例仍失败：它要求端点支持 `max_concurrency` 参数并在
  `DBA_OB_AI_MODEL_ENDPOINTS` 暴露 `MAX_CONCURRENCY` 列，这需要改内部表
  `__all_ai_model_endpoint` 的 schema；upstream master 同样没有该能力，基线上该用例也不通过，
  因此不在本次改动范围内。

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

## 三、已知限制

* **跨行批量**：当前 `AI_EMBED` 仍是逐行求值（每行一次请求），没有实现“一次请求携带多条输入”。
  OB 的表达式层提供了 batch eval 框架（`ObExpr::do_eval_batch` / `eval_batch_func_`），
  但该接口需要配套的向量化参数求值，改动面大且风险高，本次未接入；
  已完成的部分是 NULL 行不参与请求且不影响其它行的结果对齐、响应乱序按 index 归位。
* 本分支的改动未在本地编译验证（构建环境为 Linux，当前开发机是 Windows 且无 Linux/WSL/Docker），
  改动已按框架头文件逐一核对 API 签名，但请务必在 CI 或小队 Linux 机器上完成一次编译与用例回归。

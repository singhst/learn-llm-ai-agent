import asyncio
import json
from dataclasses import dataclass
from typing import Optional, AsyncGenerator, Any

# 假設你有 Ollama client
from agent_framework.ollama import OllamaChatClient

# -----------------------------
# 你的原始工具 (Skill)
# -----------------------------
def fetch_databricks_logs(run_id: str) -> str:
    """獲取指定 Databricks 任務 (Run ID) 的錯誤日誌。"""
    if "99482" in run_id:
        return (
            "Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. "
            "Reason: Data skew detected in BroadcastHashJoin optimization step. "
            "Failed task ID: task_2026_06_02."
        )
    return "Error in Databricks Job: [AnalysisException] Table or view not found: production.user_table"

# -----------------------------
# System Instructions (unchanged)
# -----------------------------
TRIAGE_INSTRUCTIONS = """
你是一個 Databricks 運維門面擔當（Triage Agent）。
你的職責是接待用戶。當收到用戶的技術診斷報告與建議後，
請將其整理並用流暢、親切的繁體中文（廣東話/粵語口語）向用戶解釋，並保持專業的排版。
"""

COLLECTOR_INSTRUCTIONS = """
你是一個專職的數據採集員（Log Collector Agent）。
當用戶提供 Run ID 時，你的唯一任務是調用 `fetch_databricks_logs` 工具來獲取錯誤日誌。
請直接輸出工具回傳的完整日誌內容，嚴禁附加任何自我的分析、評論或多餘的字眼。
"""

EXPERT_INSTRUCTIONS = """
你是一個頂尖的 Databricks/Spark 性能優化專家（Spark Expert Agent）。
請仔細閱讀輸入的錯誤日誌，找出 Root Cause（根本原因）。
如果判定為 OOM (OutOfMemoryError)，請給出具體且具建設性的 Spark 參數優化建議，並附上標準的 Markdown Code Block。
"""

# -----------------------------
# 簡單的 workflow/step decorator wrappers (替換為 MAF SDK 的 decorator)
# -----------------------------
def step(fn):
    return fn

def workflow(fn):
    return fn

# -----------------------------
# Typed messages between steps
# -----------------------------
@dataclass
class UserRequest:
    user_text: str
    run_id: Optional[str] = None

@dataclass
class RawLogs:
    run_id: str
    content: str

@dataclass
class ExpertReport:
    run_id: str
    analysis_markdown: str

@dataclass
class FinalReply:
    run_id: str
    triage_text: str

# -----------------------------
# Checkpoint helper (簡單示範)
# -----------------------------
def save_checkpoint(step_name: str, payload: dict):
    fname = f"checkpoint_{step_name}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# -----------------------------
# Helpers to handle agent.run return types
# -----------------------------
async def collect_full_response(maybe_iterable: Any) -> str:
    """
    Accept either:
    - an async iterator (streaming): iterate and concatenate chunk.text
    - a coroutine returning a response object: await and extract .text or str()
    """
    # If it's an async iterator/generator
    if hasattr(maybe_iterable, "__aiter__"):
        text = ""
        async for chunk in maybe_iterable:
            if getattr(chunk, "text", None):
                text += chunk.text
        return text

    # Otherwise assume it's a coroutine that returns a response
    resp = await maybe_iterable
    # Try common attributes
    if hasattr(resp, "text"):
        return resp.text
    # If resp is a list/tuple of chunks
    if isinstance(resp, (list, tuple)):
        parts = []
        for item in resp:
            parts.append(getattr(item, "text", str(item)))
        return "".join(parts)
    return str(resp)

async def stream_response_generator(maybe_iterable: Any) -> AsyncGenerator[str, None]:
    """
    Yield chunks as strings whether agent.run returns an async iterator or a coroutine.
    If coroutine returns a single response, yield it once.
    """
    if hasattr(maybe_iterable, "__aiter__"):
        async for chunk in maybe_iterable:
            if getattr(chunk, "text", None):
                yield chunk.text
        return

    resp = await maybe_iterable
    if hasattr(resp, "text"):
        yield resp.text
    elif isinstance(resp, (list, tuple)):
        for item in resp:
            yield getattr(item, "text", str(item))
    else:
        yield str(resp)

# -----------------------------
# Step implementations (使用 Ollama client inside steps)
# -----------------------------
@step
async def collector_step(client: OllamaChatClient, req: UserRequest) -> RawLogs:
    """
    Collector step:
    - 使用 CollectorAgent 去觸發 ReAct 並呼叫本地工具 (fetch_databricks_logs)
    - 這裡預設直接呼叫工具（更可控）
    """
    run_id = req.run_id or (req.user_text.split()[-1])
    logs = fetch_databricks_logs(run_id)

    save_checkpoint("maf-02_functional_workflows-collector", {"run_id": run_id, "content_preview": logs[:200]})
    return RawLogs(run_id=run_id, content=logs)

@step
async def expert_step(client: OllamaChatClient, raw: RawLogs) -> ExpertReport:
    """
    Expert step:
    - 呼叫 ExpertAgent（LLM）來分析 raw logs，回傳 Markdown 格式的分析報告
    - Robustly handle streaming vs non-streaming agent.run
    """
    expert_agent = client.as_agent(name="ExpertAgent", instructions=EXPERT_INSTRUCTIONS)
    prompt = f"請幫我分析以下 Databricks 錯誤日誌並給出調優代碼：\n{raw.content}"

    maybe_iter = expert_agent.run(prompt)  # could be coroutine or async iterator
    analysis = await collect_full_response(maybe_iter)

    save_checkpoint("maf-02_functional_workflows-expert", {"run_id": raw.run_id, "analysis_preview": analysis[:400]})
    return ExpertReport(run_id=raw.run_id, analysis_markdown=analysis)

@step
async def triage_step_streaming(client: OllamaChatClient, report: ExpertReport) -> AsyncGenerator[str, None]:
    """
    Triage step (streaming):
    - 呼叫 TriageAgent 並以 stream=True 逐字輸出（適合前端 streaming UX）
    - This yields chunks to the caller.
    """
    triage_agent = client.as_agent(name="TriageAgent", instructions=TRIAGE_INSTRUCTIONS)
    final_prompt = f"請將以下的技術診斷報告轉譯為親切的廣東話覆述給用戶：\n{report.analysis_markdown}"

    maybe_iter = triage_agent.run(final_prompt, stream=True)
    async for chunk_text in stream_response_generator(maybe_iter):
        yield chunk_text

    save_checkpoint("maf-02_functional_workflows-triage", {"run_id": report.run_id, "triage_done": True})

# -----------------------------
# Workflow composition
# -----------------------------
@workflow
async def databricks_debug_workflow(user_text: str, run_id: Optional[str] = None):
    """
    Functional workflow: Collector -> Expert -> Triage (streaming)
    """
    client = OllamaChatClient(model_id="gemma4:e4b")

    req = UserRequest(user_text=user_text, run_id=run_id)

    # Step 1: Collector
    raw = await collector_step(client, req)
    print(f"✅ [Log 獲取成功] Run ID: {raw.run_id}\n內容片段: {raw.content[:120]}...\n" + "-"*40)

    # Step 2: Expert
    print("⏳ [Step 2] ExpertAgent 正在進行 Spark 深度故障分析...", flush=True)
    report = await expert_step(client, raw)
    print("✅ [專家分析完成]\n" + "-"*40)

    # Step 3: Triage (streaming)
    print("⏳ [Step 3] TriageAgent 正在將報告轉譯為廣東話並輸出給用戶:\n", flush=True)
    async for chunk in triage_step_streaming(client, report):
        print(chunk, end="", flush=True)

    print("\n\n✅ [Workflow 完成]")

# -----------------------------
# Run locally for quick test
# -----------------------------
if __name__ == "__main__":
    async def main():
        user_query = "我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。"
        await databricks_debug_workflow(user_query, run_id="run_99482")

    asyncio.run(main())


#### 執行結果示例 ######################################################################################################
# $ .venv_maf/bin/python usecase-debug-spark-pipeline/test-multi_agent-maf-02_functional_workflows-debug-databricks-job.py 
# ✅ [Log 獲取成功] Run ID: run_99482
# 內容片段: Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. Reason: Data skew detected in Broadcast...
# ----------------------------------------
# ⏳ [Step 2] ExpertAgent 正在進行 Spark 深度故障分析...
# ✅ [專家分析完成]
# ----------------------------------------
# ⏳ [Step 3] TriageAgent 正在將報告轉譯為廣東話並輸出給用戶:
# 
# 各位，你好！你過來，我睇咗你份技術報告同日誌。
# 
# 你放心，呢個個問題我睇得非常清楚，**唔係單純嘅「記存爆咗」（OOM）咁簡單嘅記憶體不足問題**。我哋要深入啲睇，根本出問題嘅核心，係個**「數據分佈唔平均」**（Data Skew）嘅問題。呢個Skew直接導致資源分配嗰陣，出現咗極端嘅負載傾斜。
# 
# 等我用我平時講解嘅方式，幫你一次過拆解清楚。
# 
# ***
# 
# ### 💡 第一部分：深度診斷與核心原因（點解會爆？）
# 
# **🔍 睇點：** 呢個錯誤訊息 `GC overhead limit exceeded`，其實係一個「溫和嘅警告」，佢話你嘅垃圾回收機制（GC）已經達到極限，冇辦法再應付咁大嘅負載了。
# 
# **🔥 根本原因 (Root Cause)：**
# *   問題出喺一個叫 `BroadcastHashJoin` 嘅優化步驟。
# *   簡單嚟講，當 Spark 要做 Join 動作時，佢會試吓將較細嘅數據集「廣播」（Broadcast）到每個處理節點。
# *   點知呢個數據集，喺某個 Join Key（即係你用嚟比對嘅關鍵欄位）上，有極少數嘅 Key，佢哋帶嚟嘅資料筆數，係成個數據集嘅「罪魁禍首」，份量過大，遠超平均水平。
# 
# **🤯 結果點樣：**
# 1.  Spark 將處理呢個「負載過重嘅 Key」嘅任務（Task）分配畀某一個單一嘅 Executor 節點。
# 2.  因為個任務份量太大，單一個節點要處理嘅記憶體壓力，超過咗佢自己可以承受嘅範圍。
# 3.  結果就係個單任務，將成個系統帶到極限，最後就「炸」咗，拋出 OOM 錯誤。
# 
# **📌 總結嚟講，單單增加總記憶體（例如增加 Executor 個數）係幫唔到你，因為問題唔係「整體記憶體太少」，而係「有極少數個單任務，負載太過重」！**
# 
# ***
# 
# ### ✨ 第二部分：最佳化策略與建議（點樣解決？）
# 
# 我哋要採取嘅唔單止係「貼個補丁」（治標），而必須要「搵根本問題」（治本）。
# 
# #### 🎯 最佳化重點：雙管齊下
# 
# 1.  **【⭐ 治本首選】處理數據分佈：** 呢個係最核心，必須將數據分佈均勻化，減少個 Key 嘅「傾斜」壓力。
# 2.  **【📚 輔助補強】調整資源邊界：** 雖然根源係數據，但同時都要將 Executor 嘅資源上限提升，為應對無法完全排除嘅負載峰值預留空間。
# 
# #### 🚀 推薦嘅參數優化方向
# 
# | 參數方向 | 參數名稱 | 建議值（參考） | 目的（用廣東話講） |
# | :--- | :--- | :--- | :--- |
# | **🔑 數據傾斜** | `spark.sql.adaptive.enabled` | `true` | **最重要！** 啟用「自適應查詢執行」（AQE）。呢個係 Spark 最聰明嘅功能，佢會自動去偵測數據傾斜，並嘗試自動優化 Join 過程，帮你幫手。 |
# | **💾 資源分配** | `spark.executor.memory` | 增大 (例如 12G) | 確保每個處理節點（Executor）有足夠嘅「緩衝記憶體」，應付嗰啲瞬間嘅數據洪峰。 |
# | **⚙️ 資源分配** | `spark.executor.cores` | 適當設定 (例如 5) | 確保每個 Executor 唔單止夠記憶體，亦夠核心數，可以將任務並行處理，唔好令單核成為瓶頸。 |
# | **🤝 Join 優化** | `spark.sql.autoBroadcastJoinThreshold` | 唔一定要改 | 呢個參數負責決定邊個數據係咪夠細可以廣播。不過，由於我哋嘅問題點係 Skew，所以最好就交俾 AQE（即係 `adaptive.enabled`）去處理，唔建議自己手動去改佢。 |
# 
# ***
# 
# ### 🛠️ 第三部分：實施代碼範例（交咗畀你）
# 
# 你只需要將下面嘅參數，放入你嘅 Databricks Job 或者 Spark Session 嘅配置度，執行一次：
# 
# ```python
# # 🌟 步驟一：開大功！
# # 啟用適應式查詢執行，呢個係解決 Data Skew 最重要嘅「魔法開關」。
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# 
# # 💾 步驟二：加「底勁」，為應對極端負載預留足夠空間。
# spark.conf.set("spark.executor.memory", "12g") 
# 
# # ⚙️ 步驟三：優化核心使用率。
# spark.conf.set("spark.executor.cores", "5") 
# ```
# 
# #### ⚠️ 專業提醒與後備方案（如果以上都唔得）
# 
# 1.  **首選：** 你一定要將 `spark.sql.adaptive.enabled` 設定成 `true`，呢個係你嘅第一道防線。
# 2.  **資料預處理（最後手段）：** 如果你試咗以上所有優化，Skew 錯誤仲係頻繁發生，咁代表數據嘅傾斜程度已經超出 Spark 框架可以自底層優化嘅極限。
#     *   呢個時候，我哋就需要手動介入，將 Join Key 喺上做一個 **「加鹽」（Salting）** 嘅操作。簡單講，就係將嗰個負載過重嘅 Key $K$，分拆出多個 Key $K\_1, K\_2, \dots$ ，由多個任務嚟分攤處理壓力。
# 
# 整體嚟講，你先由 **`adaptive.enabled`** 嚟開始，如果仲唔得，再諗你個數據源係咪有冇辦法喺數據入口處就將數據分佈均勻化。
# 
# 你唔使擔心，我哋分步驟嚟解決，一定可以搞掂㗎！有咩唔明，隨時再問我！👍
# 
# ✅ [Workflow 完成]



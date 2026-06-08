import asyncio
from agent_framework.ollama import OllamaChatClient


# 1. 定義工具 (Tools) - Databricks Log Extractor Tool
def fetch_databricks_logs(run_id: str) -> str:
    """獲取指定 Databricks 任務 (Run ID) 的錯誤日誌。輸入必須為任務代碼，例如 'run_12345'。"""
    # 這裡模擬一個典型的 Spark OutOfMemory 錯誤日誌
    if "99482" in run_id:
        return (
            "Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. "
            "Reason: Data skew detected in BroadcastHashJoin optimization step."
        )
    return "Error in Databricks Job: [AnalysisException] Table or view not found: production.user_table"


# 2. Agent 核心配置 (針對 Databricks 偵錯的 ReAct 流程)
SYSTEM_INSTRUCTIONS = """
你是一個專門診斷 Databricks/Spark 任務故障的資深工程師。請遵循 ReAct 模式（Thought -> Action -> Observation）：
1. 首先，使用 `fetch_databricks_logs` 工具獲取故障日誌。
2. 分析日誌中的 Root Cause (例如 OOM 記憶體溢出、權限問題或語法錯誤)。
3. 給出最具體的 Spark 參數修復建議。
4. 最終回覆請使用繁體中文（廣東話/粵語口語）簡單解釋，並附上修復代碼區塊。
"""
# SYSTEM_INSTRUCTIONS = """
# You are a senior engineer specializing in diagnosing Databricks/Spark job failures. Please follow the ReAct pattern (Thought → Action → Observation):
# 1. First, use the fetch_databricks_logs tool to obtain the failure logs.
# 2. Analyze the Root Cause in the logs (for example, OOM memory overflow, permission issues, or syntax errors).
# 3. Provide the most specific Spark parameter fix recommendations.
# 4. In the final reply, use English to give a simple explanation, and include a repair code block.
# """


async def run_databricks_debugger():
    # 初始化 Client (連接本地 Ollama gemma4:e4b)
    client = OllamaChatClient(model_id="gemma4:e4b")

    # 建立 Agent 並綁定工具 (MAF 會自動處理 ReAct 循環)
    agent = client.as_agent(
        name="DatabricksDebugAgent",
        instructions=SYSTEM_INSTRUCTIONS,
        tools=[fetch_databricks_logs]
    )

    # 模擬輸入一個發生 OOM 的 Run ID
    query = "我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。"
    # query = "My Databricks job crashed, the Run ID is run_99482. Please help me check why it failed and how to fix it with code."

    
    # 執行並直接輸出結果串流
    thinking_buffer = ""
    async for chunk in agent.run(query, stream=True):
        for content in chunk.contents:
            if content.type == "text_reasoning":
                thinking_buffer += content.text
        if chunk.text:
            print(chunk.text, end="", flush=True)
    
    if thinking_buffer:
        print(f"\n\n[完整思考過程]\n{thinking_buffer}", flush=True)

if __name__ == "__main__":
    asyncio.run(run_databricks_debugger())
    print()



#### 執行結果示例 ######################################################################################################
# $ .venv_maf/bin/python usecase-debug-spark-pipeline/test-single-fetch-log-agent-databricks-maf.py 
# (💡 **思考過程**:
# 1.  **診斷錯誤**: 根據提供的日誌，錯誤是 `java.lang.OutOfMemoryError: GC overhead limit exceeded`。
# 2.  **定位根源**: 錯誤訊息詳細指出問題發生在 `"Data skew detected in BroadcastHashJoin optimization step."`。
# 3.  **理解問題**: 這不是單純的記憶體不足，而是**數據傾斜 (Data Skew)** 導致的記憶體爆炸。當兩張表進行 JOIN 時，某些 key 的資料量極大（數據傾斜），Spark 嘗試廣播或處理這些單一巨量鍵時，就會超出記憶體上限。
# 4.  **修復建議**: 最直接的參數修正方法是利用 Spark 3+ 的 **Adaptive Query Execution (AQE)** 功能，讓系統能更智能地檢測和修復數據傾斜問題。
# 5.  **編寫回覆**: 必須使用廣東話口吻，解釋原理，提出參數修正，並提供代碼。這是一個「高層次」的數據處理問題，不能只靠加 RAM 解決。)
# 
# ---
# 
# 嘩，你呢個 Job 爆咗，我睇咗個日誌，**個問題唔係單純嘅「爆晒記憶體」咁簡單，而係因為數據本身有結構性嘅缺陷，導致嘅記憶體溢出。**
# 
# ### 📝 診斷分析 (Diagnosis)
# 
# **根源：** `java.lang.OutOfMemoryError: GC overhead limit exceeded.`
# **真兇：** `Data skew detected in BroadcastHashJoin optimization step.`
# 
# 簡單嚟講，你做個 JOIN（連接兩張表）嗰陣，**數據唔係分佈均衡㗎**。喺兩張表嘅連接欄位（Join Key）入面，有少數幾個 Key 嘅資料量係極大嘅，呢種現象就叫 **數據傾斜 (Data Skew)**。
# 
# 當 Spark 執行 `BroadcastHashJoin` 嗰陣，就好似佢將啲數據集中到少數幾個 Key 上面，就好似將所有嘅嘢都放落同一嚿嘢上面。咁多數據份量會令到 Spark 執行器（Executor）嘅記憶體瞬秒過載（OOM）。
# 
# ### 🛠️ 參數修復建議 (Actionable Fix)
# 
# 你唔可以淨係加 RAM 嚟解決個「結構性」問題。我哋要俾 Spark 嘅優化器 (Optimizer) 幫手，喺佢發現數據傾斜時，可以自動調整 Join 策略，唔好將所有嘢一次過處理。
# 
# **建議你喺 Job 嘅 Spark 設定 (Spark Configuration) 加上或確保以下參數係開啟狀態：**
# 
# ```scala
# // 確保 Spark 3.x 或更高版本，呢個參數極重要
# spark.sql.adaptive.enabled true 
# ```
# 
# **💡 說明：**
# 當 `spark.sql.adaptive.enabled` 設為 `true` (即係啟用 AQE, Adaptive Query Execution) 後，Spark 會喺執行過程中動態監測數據分佈。當佢偵測到數據傾斜時，唔會死硬地用一個策略去處理，而會更智能地調整 Join 方式，減低單個 Key 嘅記憶體壓力，從而避免 OOM。
# 
# ### 💻 完整 Code 修正區塊
# 
# 如果你係用 Python/PySpark 運行，你應該將呢個參數加到你 Job 嘅 Configuration setting 裡面：
# 
# ```python
# # 假設你係通過 SparkSession 運行
# spark = SparkSession.builder.appName("JobName").config("spark.sql.adaptive.enabled", "true").getOrCreate()
# 
# # 之後執行你原本的 join 任務
# df_result = df1.join(df2, on="join_key", how="inner")
# 
# df_result.write.format("parquet").save("output_path")
# ```
# 
# **⚠️ 終極警告 (Ultimate Tip):**
# 如果就算加上以上參數，個 Job 仲係爆，咁就代表呢個數據傾斜問題已經超出單純嘅參數優化範圍。咁樣嘅情況，你可能需要喺**程式碼層面**進行修復，例如手動執行「Salt」（鹽化）處理：喺 Join Key 上面加一個隨機數或者重新分區 (`repartitionByRange`)，等將嗰嚿超大嘅數據拆成多嚿細嘅，再進行 JOIN。但通常，先試吓加上 AQE 參數就夠喇。
# 
# [完整思考過程]
# The user's Databricks Job failed, and they provided the Run ID: `run_99482`.
# I need to diagnose the failure using the available tool, analyze the logs to find the root cause, determine the necessary Spark parameter fix, and finally provide the explanation and code fix in traditional Chinese (spoken Cantonese/Hong Kong style).
# 
# Plan:
# 1. **Action**: Call `fetch_databricks_logs` with `run_99482`.
# 2. **Wait for Observation**: Analyze the returned logs.
# 3. **Analyze & Suggest**: Based on the logs, pinpoint the cause (e.g., OOM, permissions, code error) and suggest specific Spark parameter changes.
# 4. **Final Output**: Generate the Cantonese-style explanation and the corrected code block.The user wants me to diagnose a failed Databricks Job (`run_99482`).
# I have executed `fetch_databricks_logs` and received the error:
# `Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. Reason: Data skew detected in BroadcastHashJoin optimization step.`
# 
# **Analysis:**
# 1.  **Root Cause:** `java.lang.OutOfMemoryError: GC overhead limit exceeded.` This is a memory issue (OOM).
# 2.  **Specific Trigger:** The error is explicitly linked to `"Data skew detected in BroadcastHashJoin optimization step."`
# 3.  **Meaning:** The job is failing because a specific join operation (`BroadcastHashJoin`) encountered data skew. This means that a small number of keys in the join columns have an excessively large number of corresponding records. When Spark tries to broadcast the smaller table, the large number of records for the skewed key overwhelms the memory, leading to an OOM error.
# 
# **Solution/Fix:**
# Since the problem is data skew during a join, simply increasing general memory (`spark.executor.memory`) might only delay the crash or require massive resources. The proper fix is to handle the skew explicitly.
# 
# 1.  **Strategy 1 (Re-partitioning/Salt):** The standard fix for skew is to apply a technique like salting (adding a random prefix to the join key to break up the mega-group) or re-partitioning/using appropriate join strategies (e.g., Adaptive Query Execution (AQE) if available and configured).
# 2.  **Strategy 2 (Configuration Tuning - Less Ideal but sometimes necessary):** If I cannot change the code logic, I might try to increase resources, but the root cause is structural skew, not just resource lack.
# 
# Since I cannot modify the source code logic (which would involve re-partitioning), the best advice is to suggest enabling features that handle skew automatically or forcing a more robust join method.
# 
# **Best Technical Recommendation:**
# *   Ensure `spark.sql.adaptive.enabled` is set to `true` (if running Spark 3+) as AQE handles skew more intelligently than older versions.
# *   If AQE is not enough, the user must modify the code to detect and handle the skew before the join (e.g., using `repartitionByRange` or explicit salting).
# 
# I will structure the advice by explaining the cause (Skew leading to OOM), and then providing the most effective parameter fix (`spark.sql.adaptive.enabled`). I will also add a strong warning that if this fails, code modification (salting) is required.
# 
# **Tone Check:** Cantonese/Hong Kong colloquial style. (Casual, helpful, slightly urgent.)




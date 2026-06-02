import asyncio
from agent_framework import Agent, AgentResponseUpdate, WorkflowBuilder
from agent_framework.ollama import OllamaChatClient


# ==========================================
# 1. 定義工具 (Skill) – Collector 專用
# ==========================================
def fetch_databricks_logs(run_id: str) -> str:
    """獲取指定 Databricks 任務 (Run ID) 的錯誤日誌。"""
    if "99482" in run_id:
        return (
            "Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. "
            "Reason: Data skew detected in BroadcastHashJoin optimization step. "
            "Failed task ID: task_2026_06_02."
        )
    return "Error in Databricks Job: [AnalysisException] Table or view not found: production.user_table"


# ==========================================
# 2. Agent System Instructions
# ==========================================
TRIAGE_INSTRUCTIONS = """
You are the face of Databricks operations (Triage Agent).
Your responsibility is to assist users. Upon receiving a user's technical diagnostic report and recommendations,
organize the information and explain it to the user in fluent, friendly Traditional Chinese (Cantonese spoken style), while maintaining a professional layout.
"""

COLLECTOR_INSTRUCTIONS = """
You are a dedicated Log Collector Agent.
When the user provides a Run ID, your sole task is to call the `fetch_databricks_logs` tool to retrieve the error logs.
Output the complete log content returned by the tool directly. Do not add any of your own analysis, comments, or extra words.
"""

EXPERT_INSTRUCTIONS = """
You are a top-tier Databricks/Spark performance tuning expert (Spark Expert Agent).
Carefully read the input error logs and identify the root cause.
If the error is determined to be OOM (OutOfMemoryError), provide specific and constructive Spark parameter optimization recommendations, accompanied by a standard Markdown code block.
"""


# ==========================================
# 3. Graph-based Workflow (取代原先的手動串聯)
# ==========================================
async def main() -> None:
    # 使用本地 Ollama 模型
    client = OllamaChatClient(model_id="gemma4:e4b")

    # 建立三個 Agent，每一個都是一個圖節點
    collector_agent = Agent(
        client=client,
        name="CollectorAgent",
        instructions=COLLECTOR_INSTRUCTIONS,
        tools=[fetch_databricks_logs],       # 只有 Collector 會調用工具
    )
    expert_agent = Agent(
        client=client,
        name="ExpertAgent",
        instructions=EXPERT_INSTRUCTIONS,
    )
    triage_agent = Agent(
        client=client,
        name="TriageAgent",
        instructions=TRIAGE_INSTRUCTIONS,
    )

    # 建立線性圖：Collector → Expert → Triage
    workflow = (
        WorkflowBuilder(start_executor=collector_agent)
        .add_edge(collector_agent, expert_agent)   # Collector 的輸出 → Expert 的輸入
        .add_edge(expert_agent, triage_agent)      # Expert 的輸出  → Triage 的輸入
        .build()
    )

    # 用戶輸入（模擬 webhook 通知內容）
    user_query = "我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。"
    print(f"[User Query]: {user_query}\n" + "=" * 60)

    # 以 streaming 模式執行整個工作流
    last_author: str | None = None
    events = workflow.run(user_query, stream=True)

    async for event in events:
        if event.type == "output" and isinstance(event.data, AgentResponseUpdate):
            update = event.data
            author = update.author_name

            # 當切換到新的 Agent 時換行並印出 agent 名稱
            if author != last_author:
                if last_author is not None:
                    print()  # 不同 agent 之間留一個空行
                print(f"\n{author}: ", end="", flush=True)
                last_author = author

            # 逐 token 打印
            print(update.text, end="", flush=True)

    print()  # 最後的換行


if __name__ == "__main__":
    asyncio.run(main())




#### 執行結果示例 ######################################################################################################
# $ .venv_maf/bin/python usecase-debug-spark-pipeline/test-multi_agent-maf-03_functional_workflows-debug-databricks-job.py 
# [User Query]: 我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。
# ============================================================
# 
# CollectorAgent: 
# 
# ExpertAgent: 這是一個典型的 **記憶體不足 (OutOfMemoryError, OOM)** 錯誤，具體來說是 `GC overhead limit exceeded`，表示 JVM 嘗試回收記憶體的開銷超過了其預設限制，無法完成任務。
# 
# ### 🚀 根本原因分析 (Root Cause)
# 
# 根據錯誤訊息：`Reason: Data skew detected in BroadcastHashJoin optimization step.`
# 
# 根本原因是您的資料在執行 **Join (連接)** 操作時，發生了 **資料傾斜 (Data Skew)**。
# 
# 當 Spark 嘗試使用 `BroadcastHashJoin`（廣播雜湊連接）時，如果某些 Join Key（連接鍵）的資料分佈極不均勻（例如，某個 Key 的資料量遠大於其他 Key），Spark 會將處理這個熱點 Key 的所有資料發送到單一個 Executor 進行處理。如果這個單一個 Executor 接收到的資料量太大，就會導致該 Executor 的記憶體耗盡，進而引發 OOM。
# 
# 這是一個 **資料層面的問題**，而不是單純的資源不足，單純增加資源可能無法根本解決問題。
# 
# ### 🛠️ 解決方案與優化建議 (Optimization Recommendations)
# 
# 您需要從 **程式碼層面** 和 **Spark 配置參數層面** 兩個角度進行優化。
# 
# #### 1. 🥇 程式碼級別的修正 (必須先做)
# 
# 處理資料傾斜最有效的方法是 **「反傾斜 (Anti-skewing)」**。
# 
# *   **策略一：Key Salt (推薦)**
#     如果知道是哪一個 Key 導致了傾斜，可以在該 Key 上進行「加鹽 (Salting)」處理。這意味著在 Join 之前，將熱點 Key 的資料隨機拆分到多個不同的 Key 上，然後在 Join 的另一方（通常是小表或查找表）也進行相應的處理。
# *   **策略二：限制 Join Key**
#     如果資料集太大，考慮是否可以減少 Join Key 的數量，或使用更具代表性的 Key 進行 Join。
# *   **策略三：優化 JOIN 順序**
#     確保總是讓小型資料集（小表）進行廣播 (Broadcast)，這樣能最大程度減少網路傳輸和記憶體壓力。
# 
# #### 2. 🥈 Spark 參數級別的修正 (輔助優化)
# 
# 如果無法立刻修改程式碼進行 Anti-skewing，可以嘗試以下參數調整，以增加記憶體容忍度，但這只能治標，不能治本。
# 
# *   **`spark.executor.memory`:** 增加每個 Executor 的記憶體。
# *   **`spark.sql.shuffle.partitions`:** 增加 Shuffle 分區的數量。如果分區太少，資料就會集中到少數幾個 Executor 上。
# *   **使用 `spark.sql.adaptive.query.execution.enabled` (推薦):** 啟用自適應查詢執行模式。這讓 Spark 可以在運行時偵測到資料傾斜，並自動調整 Join 策略（例如，在偵測到傾斜時，可能自動切換到更穩健的 Join 方式），這是一個現代 Spark 環境下的極佳優化點。
# 
# ---
# 
# ### 💻 實施建議 (Recommended Code Block)
# 
# 請您按照您的環境，將以下參數應用到您的 Databricks Job 或 Notebook Session 上：
# 
# ```python
# # 步驟一：開啟自適應查詢執行模式 (強烈推薦，應放在 Notebook 開頭)
# spark.conf.set("spark.sql.adaptive.query.execution.enabled", "true")
# 
# # 步驟二：增加 Executor 資源，應根據您的資料規模決定一個合理的增幅
# # 假設您目前的 Executor 記憶體不足，請增加它。
# spark.conf.set("spark.executor.memory", "8g") # 根據需要調整，例如 "8g"
# spark.conf.set("spark.executor.cores", "4")   # 確保每個 Executor 有足夠的資源
# 
# # 步驟三：增加 Shuffle 分區數量，分散資料分佈
# # 增加到核心數的數倍，以防資料集中化
# spark.conf.set("spark.sql.shuffle.partitions", "2000")
# 
# # 【最終的優化步驟】
# # 如果你知道具體的 Join Key 'skew_key' 導致傾斜，請務必在 Join 前進行手動處理，
# # 例如使用 Python/PySpark 函數實作 Salting 邏輯，這是最可靠的解決方案。
# ```
# 
# **總結：** 請先實作 **自適應查詢執行 (Adaptive Query)** 和 **增加資源 (Spark Params)** ；如果 Job 仍失敗，請立即改寫程式碼，使用 **Key Salt** 或其他 **Anti-skewing** 技術處理資料傾斜。
# 
# TriageAgent: 這是一個典型的 **記憶體不足 (OutOfMemoryError, OOM)** 錯誤，具體來說是 `GC overhead limit exceeded`，表示 JVM 嘗試回收記憶體的開銷超過了其預設限制，無法完成任務。
# 
# ### 🚀 根本原因分析 (Root Cause)
# 
# 根據錯誤訊息：`Reason: Data skew detected in BroadcastHashJoin optimization step.`
# 
# 根本原因出在您的資料在執行 **Join (連接)** 操作時，發生了 **資料傾斜 (Data Skew)**。
# 
# 這是一個非常常見但需要深入理解的 Spark 性能問題。簡單來說，它的機制是這樣：
# 
# 1.  **廣播連接 (BroadcastHashJoin)**：Spark 預設會嘗試將小資料表（Small Table）的整個數據集，一次性廣播（Broadcast）給所有計算節點 (Executor)。
# 2.  **資料傾斜 (Data Skew)**：如果您的 Join Key（連接鍵）分佈極不均勻，也就是說，某些連接鍵（例如：某個 `user_id` 或 `product_id`）擁有的資料量遠遠超過其他 Key，這些「熱點 Key」的資料量就會非常龐大。
# 3.  **OOM 爆發**：Spark 會將所有處理這個熱點 Key 的資料，全部集中到單一個計算節點 (Executor) 進行處理。如果這個單一個 Executor 接收到的資料體積超出了其分配的記憶體極限，就會導致記憶體耗盡，觸發 `OutOfMemoryError`。
# 
# 這是一個 **資料分佈層面的問題**，而不是單純的「記憶體不足」，所以單純增加 Executor 資源可能無法從根本上解決這個問題。
# 
# ### 🛠️ 解決方案與優化建議 (Optimization Recommendations)
# 
# 我們需要從 **程式碼優化 (Code Level)** 和 **配置優化 (Config Level)** 兩個層面著手，由最重要到輔助順序處理。
# 
# #### 1. 🥇 程式碼級別的修正 (最重要，必須先做)
# 
# 處理資料傾斜的終極辦法是進行 **「反傾斜 (Anti-skewing)」** 技術。
# 
# *   **策略一：Key Salting (推薦)**
#     這是最標準有效的解決方案。如果知道是哪一個 Key 導致了傾斜 (例如：`ID = 123` 實在太熱)，可以在 Join 發生前，將這個熱點 Key 的資料在 Join Key 上進行「加鹽 (Salting)」處理。這會將一個大的 Key (e.g., `123`) 拆分成多個小型 Key (e.g., `123_1`, `123_2`, ...)，然後讓 Spark 將這些資料分散到多個 Executor 上同時處理，從而避免單點爆載。
# *   **策略二：調整 JOIN 策略**
#     如果資料表結構允許，可以手動使用更穩健的 Join 方式，或根據資料分佈重新調整 Join Key 的選擇。
# 
# #### 2. 🥈 Spark 參數級別的修正 (配置輔助，幫助分散壓力)
# 
# 這些配置是為了讓 Spark 在運行時更聰明、更分散壓力。
# 
# *   **啟用自適應查詢 (Adaptive Query Execution, AQE) (極力推薦)**
#     這是 Spark 2.x 後面極為重要的功能。它讓 Spark 在運行時（Runtime）能自動偵測到資料傾斜，並自動調整 Join 策略（例如，自動切換到更穩健的 Join 方式），極大程度地提高了容錯率。
# *   **增加 Shuffle 分區數量 (`spark.sql.shuffle.partitions`)**
#     增加分區數的目的是讓資料分散到更多不同的 Executor 上。如果預設值太少，資料會集中到少數幾個位元組上。
# *   **優化資源分配 (`spark.executor.memory`)**
#     雖然不是根本原因，但增加每個 Executor 的記憶體，能給系統一定的「緩衝區」，讓它能應對稍大一點的臨時數據集。
# 
# ---
# 
# ### 💻 實作建議 (Actionable Code Block)
# 
# 建議您將以下這組配置，作為您 Job 或 Notebook 的開頭代碼塊運行。這會讓 Spark 在運行時具備最高的容錯能力。
# 
# ```python
# # =============================================================
# # ⚙️ 🌟 階段一：優化配置 (配置 Job 參數)
# # =============================================================
# 
# # 1. 【最佳解】啟用自適應查詢，讓 Spark 自動修正資料傾斜問題
# spark.conf.set("spark.sql.adaptive.query.execution.enabled", "true")
# 
# # 2. 增加 Executor 資源 (請根據您的叢集大小調整數值)
# # 假設您需要更高的記憶體容忍度
# spark.conf.set("spark.executor.memory", "10g")
# 
# # 3. 分散化資料分佈，將資料分區數提升到更高的標準
# # 根據您的資料量，設定到 1000~5000 之間是一個較安全的區間
# spark.conf.set("spark.sql.shuffle.partitions", "2000")
# 
# # =============================================================
# # 🛠️ 階段二：程式碼層面優化 (若上述配置仍失敗)
# # =============================================================
# 
# # **‼️ 當配置優化後仍失敗時，請務必執行以下手動修正：**
# # 假設您的 Join Key 是 'product_id'，且它是造成傾斜的元兇。
# # 必須在 Join 之前，將資料進行「加鹽 (Salting)」處理：
# 
# # Example Code (Pseudo-Code)
# # 1. 讀入資料 A 和 B
# # df_A = spark.read.format(...)
# # df_B = spark.read.format(...)
# 
# # 2. 處理傾斜的列 (假設 Product_id 需要處理)
# # 從 df_A 的熱點 Key 所在資料框上，創建一個隨機 Salt 欄位，例如 :
# # df_A_salted = df_A.withColumn("salted_key", concat(col("product_id"), lit("_"), col(F.expr("rand()"))))
# # df_B_salted = df_B.withColumn("salted_key", concat(col("product_id"), lit("_"), col(F.expr("rand()"))))
# 
# # 3. 使用新的 Salted Key 進行 Join
# # final_df = df_A_salted.join(df_B_salted, on="salted_key", how="inner")
# 
# ```
# 
# **🌟 總結重點提醒：**
# 
# *   **優先順序：** 先配置參數 (`spark.conf.set(...)`) → 再執行程式碼優化 (`Anti-skewing`)。
# *   **核心解決方案：** 掌握 **資料傾斜 (Data Skew)** 的概念，並實作 **Key Salting** 是解決此類 OOM 錯誤最專業可靠的方法。






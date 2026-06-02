import asyncio
from agent_framework.ollama import OllamaChatClient

# ==========================================
# 1. 定義工具 (Skills) - 只有 Collector 會用到
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
# 2. 拆分每個 Agent 的 System Instructions
# ==========================================
# Triage Agent 只需要專注在引導與繁體中文（廣東話）的親切對答
# TRIAGE_INSTRUCTIONS = """
# 你是一個 Databricks 運維門面擔當（Triage Agent）。
# 你的職責是接待用戶。當收到用戶的技術診斷報告與建議後，
# 請將其整理並用流暢、親切的繁體中文（廣東話/粵語口語）向用戶解釋，並保持專業的排版。
# """
TRIAGE_INSTRUCTIONS = """
You are the face of Databricks operations (Triage Agent).
Your responsibility is to assist users. Upon receiving a user's technical diagnostic report and recommendations,
organize the information and explain it to the user in fluent, friendly Traditional Chinese (Cantonese spoken style), while maintaining a professional layout.
"""

# Collector Agent 不需要懂 Spark 優化，它的唯一大腦是用來觸發 Tool 拿 Log
# COLLECTOR_INSTRUCTIONS = """
# 你是一個專職的數據採集員（Log Collector Agent）。
# 當用戶提供 Run ID 時，你的唯一任務是調用 `fetch_databricks_logs` 工具來獲取錯誤日誌。
# 請直接輸出工具回傳的完整日誌內容，嚴禁附加任何自我的分析、評論或多餘的字眼。
# """
COLLECTOR_INSTRUCTIONS = """
You are a dedicated Log Collector Agent.
When the user provides a Run ID, your sole task is to call the `fetch_databricks_logs` tool to retrieve the error logs.
Output the complete log content returned by the tool directly. Do not add any of your own analysis, comments, or extra words.
"""

# Expert Agent 不需要任何工具，專心充當 Spark 知識庫，Context 非常乾淨
# EXPERT_INSTRUCTIONS = """
# 你是一個頂尖的 Databricks/Spark 性能調優專家（Spark Expert Agent）。
# 請仔細閱讀輸入的錯誤日誌，找出 Root Cause（根本原因）。
# 如果判定為 OOM (OutOfMemoryError)，請給出具體且具建設性的 Spark 參數優化建議，並附上標準的 Markdown Code Block。
# """
EXPERT_INSTRUCTIONS = """
You are a top-tier Databricks/Spark performance tuning expert (Spark Expert Agent).
Carefully read the input error logs and identify the root cause.
If the error is determined to be OOM (OutOfMemoryError), provide specific and constructive Spark parameter optimization recommendations, accompanied by a standard Markdown code block.
"""


# ==========================================
# 3. 核心 Multi-Agent 串聯邏輯
# ==========================================
async def run_multi_agent_debugger():
    # 初始化本地 Ollama gemma4:e4b Client
    client = OllamaChatClient(model_id="gemma4:e4b")

    # 建立 3 個獨立的專職 Agent
    triage_agent = client.as_agent(name="TriageAgent", instructions=TRIAGE_INSTRUCTIONS)
    collector_agent = client.as_agent(name="CollectorAgent", instructions=COLLECTOR_INSTRUCTIONS, tools=[fetch_databricks_logs])
    expert_agent = client.as_agent(name="ExpertAgent", instructions=EXPERT_INSTRUCTIONS)

    # 模擬用戶在前端的輸入
    # user_query = "我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。"
    # print(f"[User Query]: {user_query}\n" + "="*60)
    job_run_id_in_webhook_alert_content = "Run ID = run_99482"
    print(f"[User Query]: {webhook_alert_content}\n" + "="*60)

    query = f"我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。"

    # ----------------------------------------------------
    # Step 1: 讓 Collector Agent 負責去跑 ReAct 並觸發工具拿 Log
    # ----------------------------------------------------
    print("⏳ [Step 1] CollectorAgent 正在調用 API 獲取日誌...", flush=True)
    raw_logs = "<DEFAULT EMPTY>"
    # 我們不 Stream 過程，直接拿最終結果來做資料傳遞
    result = await collector_agent.run(f"請幫我撈取這個 Run ID 的日誌: {webhook_alert_content}")
    if result.text:
        raw_logs = result.text
    
    print(f"✅ [Log 獲取成功]\n內容片段: {raw_logs[:120]}...\n" + "-"*40)

    # ----------------------------------------------------
    # Step 2: 將拿到的 Raw Log 塞給 Expert Agent 進行純文字技術分析
    # ----------------------------------------------------
    print("⏳ [Step 2] ExpertAgent 正在進行 Spark 深度故障分析...", flush=True)
    expert_analysis = ""
    result = await expert_agent.run(f"請幫我分析以下 Databricks 錯誤日誌並給出調優代碼：\n{raw_logs}")
    if result.text:
        expert_analysis = result.text
            
    print(f"✅ [專家分析完成]\n內容片段: {expert_analysis[:120]}...\n" + "-"*40)

    # ----------------------------------------------------
    # Step 3: 將技術報告交給 Triage Agent 進行廣東話包裝與最終 Stream 輸出
    # ----------------------------------------------------
    print("⏳ [Step 3] TriageAgent 正在將報告轉譯為廣東話並輸出給用戶:\n", flush=True)
    
    final_prompt = f"請將以下的技術診斷報告轉譯為親切的廣東話覆述給用戶：\n{expert_analysis}"
    
    # 如果要逐字 stream，就要用 stream=True，呢度先用 await 拿完整結果
    technical_diagnostic_report = ""
    result = await triage_agent.run(final_prompt, 
                                    # stream=True
                                   )
    if result.text:
        technical_diagnostic_report = result.text

    print(f"✅ [技術診斷報告完成]\n內容片段: {technical_diagnostic_report}\n" + "-"*40)

if __name__ == "__main__":
    asyncio.run(run_multi_agent_debugger())
    print()


#### 執行結果示例 ######################################################################################################
# $ .venv_maf/bin/python test-multi_agent-maf\ copy.py 
# [User Query]: 我個 Databricks Job 爆咗，Run ID 係 run_99482。幫我睇下因咩事 fail 同埋點 code 執執佢。
# ============================================================
# ⏳ [Step 1] CollectorAgent 正在調用 API 獲取日誌...
# ✅ [Log 獲取成功]
# 內容片段: Error in Databricks Job: java.lang.OutOfMemoryError: GC overhead limit exceeded. Reason: Data skew detected in Broadcast...
# ----------------------------------------
# ⏳ [Step 2] ExpertAgent 正在進行 Spark 深度故障分析...
# ✅ [專家分析完成]
# 內容片段: 您好，我是您的 Spark 性能調優專家。
# 
# 根據您提供的錯誤日誌，我們可以非常明確地判斷出 **根本問題 (Root Cause)** 和 **觸發條件 (Trigger)**。
# 
# ---
# 
# ### 🔬 專家診斷 (Expert Diag...
# ----------------------------------------
# ⏳ [Step 3] TriageAgent 正在將報告轉譯為廣東話並輸出給用戶:
# 
# ✅ [技術診斷報告完成]
# 內容片段: 👨‍💻✨ **[Databricks Triage Agent 啟動]** ✨👩‍💻
# *(請放心，我會幫您將呢份好技術性、好難睇嘅報告，用最親切、最易明嘅廣東話，徹底拆解清楚。)*
# 
# ***
# 
# ## 👋 您好！關於您個 Spark 性能問題嘅分析報告
# 
# 各位，唔使擔心。我睇咗您交過嘅所有日誌，已經幫您將呢個性能問題，由「現象」追到「病根」埋。
# 
# 整體嚟講，呢個問題唔係單純「塞車」或者「記憶體唔夠」咁簡單，佢係一個**數據分佈本身嘅根本性問題**。我會將整個分析流程，用三個步驟帶您睇晒：**點解爆？** $\rightarrow$ **點解要爆？** $\rightarrow$ **點樣穩陣咁治？**
# 
# ---
# 
# ### 🔬 第一部分：專家診斷（點解爆？）
# 
# #### 🔍 1. 錯誤症狀 (The Symptoms)：記憶體爆咗！
# *   **專業術語：** `java.lang.OutOfMemoryError: GC overhead limit exceeded.`
# *   **廣東話解釋：** 呢個最直白嘅情況就係——**「記憶體爆晒咗」**！
# *   **意思係乜嘢？** 簡單嚟講，當 Spark 嘅計算過程開始，佢分配俾個任務嘅所有記憶體（RAM）全部用晒咗，就算垃圾收集機制（GC）再努力清理，都追唔上佢本身嘅運算需求，就會「爆機」返嚟，出現呢個 OOM 錯誤。
# 
# #### 🔍 2. 病根分析 (The Root Cause)：資料分佈唔均勻 (Data Skew)
# *   **關鍵診斷點：** `Data skew detected in BroadcastHashJoin optimization step.`
# *   **核心問題：** 呢個問題嘅真兇，就係「**數據傾斜**」（Data Skew）。
# *   **用生活化比喻嚟講：** 想像您喺一個大班朋友入面做 Group Project。如果呢個 Group Project 嘅資料，全部都靠某一個人（某一個 Join Key）去處理，而呢個人又係一個單人（單一 Executor/Task），佢就一定會因為負擔過重而「崩潰」。
# *   **點解會爆？** 您的程式碼做咗一個叫 `BroadcastHashJoin`（廣播連接）嘅優化。呢個機制本來係好快，但當數據發生傾斜時，代表喺成千上萬條記錄入面，有某幾個「熱點 Key 值」（例如，某一個特定的 `UserID=1`，但呢個用戶嘅記錄佔咗總記錄嘅 50% 以上），嘅數據量實在係太大。Spark 就將呢堆所有屬於呢個「熱點」嘅數據，全部塞定喺**一個單一嘅計算任務 (Task)** 處理。個單任務即刻記憶體就會爆晒，所以就爆咗機。
# 
# ---
# 
# ### 🛠️ 第二部分：調優建議（點樣治？）
# 
# 由於問題嘅根源係 **「數據傾斜」**，如果我淨係將記憶體參數加多，只係幫個「爆機」嘅任務增加咗一個更大的「緩衝空間」，但係根本問題（負擔過重）係冇改，所以呢種方法只係「延遲性嘅爆機」。
# 
# 我哋必須從兩個層面，由淺入深咁去處理。
# 
# #### 🚀 步驟一：資源配置調整（輕度處理 / 戰術性緩解）
# *   **目的：** 增加系統整體嘅「耐力」，為爆晒嘅任務爭取更多時間。
# *   **建議行動：** 提升以下幾個參數。呢個係一個**短期嘅「止血」方法**。
# 
# ```scala
# spark.conf.set("spark.executor.memory", "16g")  // 📈 增加單個計算機嘅記憶體
# spark.conf.set("spark.executor.cores", "4")     // 🧠 增加計算核心數量，分散負擔
# spark.conf.set("spark.driver.memory", "8g")      // 👨‍💻 確保指揮中心（Driver）有足夠記憶體處理結果
# spark.conf.set("spark.sql.shuffle.partitions", "500") // 🗂️ 將數據分成更多小份，唔好將責任集中喺單一任務
# ```
# *   **專業備註：** 呢個係一個必做嘅步驟，但請記住，呢個只係「加咗個墊」，唔係解決問題本身。
# 
# #### 🔄 步驟二：代碼邏輯重構 (Key Salting)（治本/最重要）
# *   **建議行動：** 實施 **Key Salting（鍵加鹽）** 技術。
# *   **核心原理：** 呢個方法係針對「數據傾斜」最標準、最有效嘅解決方案。我哋唔係直接處理原始嘅熱點 Key，而係喺呢個熱點 Key 上面，**人工增加一個隨機嘅「標籤」或「鹽值」（Salt）**。
# *   **比喻：** 以前所有屬於 `UserID=1` 嘅資料，全部堆晒喺一個地方（一個 Key）。我哋會將佢分成 10 份，加上一個標籤：`UserID=1_Salt_1`，`UserID=1_Salt_2`... 直到 `UserID=1_Salt_10`。
# *   **效果：** 以前一個 Task 要處理嘅巨大數據量，，會被強制分散到 10 個或更多嘅獨立 Task 上去處理。個單任務唔會再承受過度負擔，從而徹底解決 OOM 嘅根本原因。
# *   **實施指導（PySpark 舉例）：**
#     *   您需要將原來的 Join Key 轉型，在 Key 後面加上 `_` + 隨機數字。
#     *   將兩邊嘅 DataFrame 都用呢種方法進行改造，再用新的 `salted_key` 進行連接。
# 
# ---
# 
# ### ✨ 總結與執行優先順序（行動計劃）
# 
# | 優先級 | 解決方案 | 解決咗邊啲問題？ | 難度 | 效果預期 |
# | :---: | :--- | :--- | :---: | :--- |
# | **★ ★ ★** | **Key Salting (代碼重構)** | **根本問題：數據傾斜** | 高 (需修改 Join 邏輯) | **極高** (徹底解決 OOM) |
# | **★ ★** | 增加資源參數 (Resource Tuning) | **現象：記憶體不足** | 低 (只係改配置檔) | 中等 (只能緩解，無法治本) |
# | **★** | 檢查 Join 類型 | 確保 Spark 能正確利用優化器 | 中 | 高 (避免不當優化) |
# 
# **💡 最終最佳實踐建議：**
# 
# 1.  **最緊要嘅係：** 必須優先從 **步驟二（Key Salting）** 開始重構您嘅代碼邏輯，呢個係治病根嘅方法。
# 2.  **同時做：** 配合 **步驟一（增加資源參數）** 進行調優，等系統有足夠嘅緩衝空間，確保重構之後運算嘅過程係穩陣嘅。
# 
# 總結嚟講，請將焦點放在 **「如何分散熱點 Key 嘅負擔」**，咁樣可以確保您的 Spark 任務可以穩定、高效咁運行。如果對 Key Salting 呢個概念有任何疑問，請隨時再問我，我會再用最簡單嘅方式為您解釋！💪✨
# ----------------------------------------
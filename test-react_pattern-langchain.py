"""
pip install langchain-ollama langchain
"""


import os
from langchain_ollama import ChatOllama
from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.tools import tool
from langchain import hub

# 關閉 LangSmith 警告 (如果你沒用到它)
os.environ["LANGCHAIN_TRACING_V2"] = "false"

# 1. 定義工具
@tool
def get_current_price(ticker: str) -> float:
    """獲取股票當前價格。輸入應僅為股票代碼，例如 'NVDA'。"""
    prices = {"NVDA": 130.0, "AAPL": 180.0}
    
    # 更強力的字串清洗：只保留英文字母
    import re
    clean_ticker = re.sub(r'[^a-zA-Z]', '', ticker).upper()
    
    # 偵錯用：看看模型到底傳了什麼進來
    # print(f"DEBUG: 模型傳入的原始字串是 '{ticker}'，清洗後為 '{clean_ticker}'")
    
    return float(prices.get(clean_ticker, 100.0))

@tool
def calc_gain(input_str: str) -> str:
    """
    計算獲利百分比。
    輸入必須是一個包含兩個數字並用逗號分隔的字串，格式為 '買入價,現價'。
    例如：'110,130'。
    """
    try:
        # 移除可能由模型生成的引號
        clean_input = input_str.strip("'\" ")
        buy_price_str, current_price_str = clean_input.split(",")
        
        buy_price = float(buy_price_str)
        current_price = float(current_price_str)
        
        if buy_price == 0: return "買入價不能為 0"
        
        gain = ((current_price - buy_price) / buy_price) * 100
        return f"{gain:.2f}%"
    except Exception as e:
        return f"解析錯誤：請確保輸入格式為 '數字,數字'。錯誤訊息：{str(e)}"


tools = [get_current_price, calc_gain]

# 2. 初始化 Ollama 模型 (Qwen 3.5)
llm = ChatOllama(model="qwen3.5:9b", temperature=0) # 請確保名稱與你本地下載的一致

# 3. 取得 ReAct Prompt 模板
prompt = hub.pull("hwchase17/react")
# print(f"\n\n>>> prompt: `{prompt}`\n\n")

# 4. 構建 Agent 核心邏輯
agent = create_react_agent(llm, tools, prompt)

# 5. 建立 Executor (執行迴圈)
agent_executor = AgentExecutor(
    agent=agent, 
    tools=tools, 
    verbose=True,
    handle_parsing_errors=True, # 讓模型在解析錯誤時能自我修正
    max_iterations=5            # 防止無限迴圈
)

# 6. 執行任務
# 模擬情境：我以 110 元買入 NVDA，現在賺了多少？
input_query = "I bought NVDA at $110. What is my current profit percentage? Explain briefly"
print("\n--- 問題 ---")
print(input_query, "\n")
res = agent_executor.invoke({"input": input_query})
print("\n--- 最終結果 ---")
print(res)
print()
print()



### TESTING ###############################################
# 
# $ python test-react_pattern-langchain.py
# /xxx/.venv_langchain/lib/python3.8/site-packages/langsmith/client.py:241: LangSmithMissingAPIKeyWarning: API key must be provided when using hosted LangSmith API
#   warnings.warn(

# --- 問題 ---
# I bought NVDA at $110. What is my current profit percentage? Explain briefly 

# Error in StdOutCallbackHandler.on_chain_start callback: AttributeError("'NoneType' object has no attribute 'get'")
# Thought: I need to get the current price of NVDA first to calculate the profit percentage.
# Action: get_current_price
# Action Input: NVDA130.0Thought: Now I need to calculate the profit percentage using the buy price of $110 and current price of $130.
# Action: calc_gain
# Action Input: 110,13018.18%Thought: I now know the final answer  
# Final Answer: Your current profit percentage is 18.18%. This is calculated based on the current NVDA price of $130 compared to your buy price of $110.

# > Finished chain.

# --- 最終結果 ---
# {'input': 'I bought NVDA at $110. What is my current profit percentage? Explain briefly', 'output': 'Your current profit percentage is 18.18%. This is calculated based on the current NVDA price of $130 compared to your buy price of $110.'}
#
import asyncio
import re
from agent_framework.ollama import OllamaChatClient

# 1. 定義工具 (Skills)
def get_current_price(ticker: str) -> float:
    """獲取股票當前價格。輸入應僅為股票代碼，例如 'NVDA'。"""
    prices = {"NVDA": 130.0, "AAPL": 180.0}
    clean_ticker = re.sub(r'[^a-zA-Z]', '', ticker).upper()
    return float(prices.get(clean_ticker, 100.0))

def calc_gain(buy_price: float, current_price: float) -> str:
    """根據買入價和現價計算獲利百分比。"""
    if buy_price == 0: return "買入價不能為 0"
    gain = ((current_price - buy_price) / buy_price) * 100
    return f"{gain:.2f}%"

# 2. Agent 核心配置
SYSTEM_INSTRUCTIONS = """
你是一個專業的財務助手，請遵循 ReAct 模式（Thought -> Action -> Observation）：
1. 先查詢現價。 2. 再計算獲利。 3. 最後解釋結果。
"""

async def run_minimal_maf():
    # 初始化 Client (連接本地 Ollama)
    # client = OllamaChatClient(model_id="qwen3.5:9b")
    client = OllamaChatClient(model_id="gemma4:e4b")

    # 建立 Agent 並綁定工具 (MAF 會自動處理 ReAct 循環)
    agent = client.as_agent(
        name="FinanceAgent",
        instructions=SYSTEM_INSTRUCTIONS,
        tools=[get_current_price, calc_gain]
    )

    query = "I bought NVDA at $110. What is my current profit percentage? Show the percentage and explain briefly in Cantonese Traditional Chinese."
    
    # 執行並直接輸出結果串流 (包含思考過程)
    thinking_buffer = ""
    async for chunk in agent.run(query, stream=True):
        # 累積思考過程
        for content in chunk.contents:
            if content.type == "text_reasoning":
                thinking_buffer += content.text
        # 打印最終回應
        if chunk.text:
            print(chunk.text, end="", flush=True)
    
    # 完成後打印完整的思考過程
    if thinking_buffer:
        print(f"\n\n[完整思考過程]\n{thinking_buffer}", flush=True)

if __name__ == "__main__":
    asyncio.run(run_minimal_maf())
    print()


#### 執行結果示例 ######################################################################################################
# $ .venv_maf/bin/python test-react_pattern-maf.py
# 根據目前的市場數據，您的NVDA投資目前的獲利百分比是 **18.18%**。
# 
# **簡單解釋 (粵語):**
# 
# 您之前以每股 $110 嘅價格買入 NVDA。而目前嘅市價係 $130.0。咁計算落去，您嘅投資目前係賺咗約 **18.18%** 嘅盈利。恭喜您！
# 
# [完整思考過程]
# The user wants to calculate the current profit percentage for NVDA, given a purchase price of $110.
# The process involves three steps, as per the instructions:
# 1. Get the current price of NVDA. (Action: `get_current_price`)
# 2. Calculate the profit percentage using the purchase price ($110) and the current price. (Action: `calc_gain`)
# 3. Explain the result and the percentage in Cantonese Traditional Chinese.
# 
# Step 1: Get the current price of NVDA.Now I have the current price: $130.0.
# The purchase price was $110.
# Step 2: Calculate the profit percentage using `calc_gain`.I have successfully completed the calculations:
# Current Price (Observation 1): $130.0
# Profit Percentage (Observation 2): 18.18%
# 
# Step 3: Explain the result and the percentage in Cantonese Traditional Chinese.
# 
# The final output must be a comprehensive explanation following the steps taken.
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
    client = OllamaChatClient(model_id="qwen3.5:9b")

    # 建立 Agent 並綁定工具 (MAF 會自動處理 ReAct 循環)
    agent = client.as_agent(
        name="FinanceAgent",
        instructions=SYSTEM_INSTRUCTIONS,
        tools=[get_current_price, calc_gain]
    )

    query = "I bought NVDA at $110. What is my current profit percentage? Explain briefly."
    
    # 執行並直接輸出結果串流
    async for chunk in agent.run(query, stream=True):
        if chunk.text:
            print(chunk.text, end="", flush=True)

if __name__ == "__main__":
    asyncio.run(run_minimal_maf())
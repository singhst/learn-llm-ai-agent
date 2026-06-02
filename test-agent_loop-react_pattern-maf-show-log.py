import asyncio
import os
from datetime import datetime
from agent_framework.ollama import OllamaChatClient

# --- 1. 工具定義 ---
def get_current_location() -> str:
    return "Hong Kong"

def get_current_date() -> str:
    return f"Today is {datetime.now().strftime('%Y-%m-%d')}."

def get_historical_weather(location: str, date: str) -> str:
    return f"The weather in {location} on {date} was Rainy, 24°C."

# --- 2. 原始流式監控器 (使用 ANSI Escape Code 標註 Action) ---
class RawStreamMonitor:
    def __init__(self):
        pass

    async def monitor(self, agent_gen):
        try:
            async for chunk in agent_gen:
                # --- 1. 抓取所有內容片段 ---
                contents = getattr(chunk, 'contents', [])
                role = getattr(chunk, 'role', '')
                text = getattr(chunk, 'text', "")

                # --- 2. 暴力監控：如果有 text 就印，沒有就印 contents ---
                if text:
                    # 這是普通的 Thought 或 Final Answer
                    print(text, end="", flush=True)
                
                for item in contents:
                    # A. 捕獲 Action (工具調用)
                    func = getattr(item, 'function', None)
                    if func:
                        # 這是最重要的 Tracing！
                        print(f"\n[ACTION] {func.name}({func.arguments})")
                        continue

                    # B. 捕獲 Observation (執行結果)
                    # 檢查 item 是否有 content 或 output 屬性
                    obs = getattr(item, 'content', getattr(item, 'output', None))
                    if role == 'tool' and obs:
                        print(f"\n[OBSERVATION] {obs}\n")

                # --- 3. 備援 Debug (如果還是看不到，取消註釋下面這行) ---
                # print(f"DEBUG: {chunk}")

        except Exception as e:
            print(f"\nTrace Error: {e}")

async def main():
    # 關閉內建日誌避免干擾
    os.environ["ENABLE_CONSOLE_EXPORTERS"] = "false"
    
    # 修正：OllamaChatClient 初始化不帶 options
    # client = OllamaChatClient(model_id="qwen2.5:7b")
    # client = OllamaChatClient(model_id="qwen3.5:9b")
    client = OllamaChatClient(model_id="gemma4:e4b")
    
    # 修正：參數應該放在這裡 (或是根據你的 SDK 版本，可能需要直接在 Ollama 模型端調校)
    agent = client.as_agent(
        name="WeatherAssistant",
        instructions="你是一個 ReAct 助手。必須先呼叫工具獲取位置與日期。請用繁體中文回答。",
        tools=[get_current_location, get_current_date, get_historical_weather]
    )
    
    monitor = RawStreamMonitor()
    print("=== 強制穩定監控模式 (修正版) ===\n")
    
    # 開始運行
    await monitor.monitor(agent.run("我在哪裡？這裡昨天的天氣如何？", stream=True))

if __name__ == "__main__":
    asyncio.run(main())
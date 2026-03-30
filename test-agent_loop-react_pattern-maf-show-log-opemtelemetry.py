import asyncio
import os
from datetime import datetime

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from agent_framework.ollama import OllamaChatClient

# --- 1. 配置 OpenTelemetry ---
def setup_tracing():
    provider = TracerProvider()
    # 使用 SimpleSpanProcessor 確保即時在終端機印出 JSON
    processor = SimpleSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return provider

# --- 2. 工具定義 ---
def get_current_location() -> str:
    return "Hong Kong"

def get_current_date() -> str:
    return f"Today is {datetime.now().strftime('%Y-%m-%d')}."

def get_historical_weather(location: str, date: str) -> str:
    return f"The weather in {location} on {date} was Rainy, 24°C."

# --- 3. 執行主體 ---
async def main():
    os.environ["ENABLE_CONSOLE_EXPORTERS"] = "true"
    tp = setup_tracing()
    
    # 獲取當前模組的 tracer
    tracer = trace.get_tracer(__name__)

    client = OllamaChatClient(model_id="qwen3.5:9b")
    
    agent = client.as_agent(
        name="WeatherAssistant",
        instructions="你是一個 ReAct 助手。必須呼叫工具獲取位置，然後用繁體中文回答。",
        tools=[get_current_location, get_current_date, get_historical_weather]
    )

    print("=== 手動包裹 OpenTelemetry 追蹤 ===\n")

    # --- 關鍵整合部分 ---
    # 使用 start_as_current_span 建立一個頂層容器
    with tracer.start_as_current_span("AgentExecution") as span:
        # 你可以在這裡加入自定義標籤 (Attributes)
        span.set_attribute("user.query", "我在哪裡？這裡昨天的天氣如何？")
        
        response = await agent.run("我在哪裡？這裡昨天的天氣如何？", stream=True)
        
        async for chunk in response:
            if chunk.text:
                print(chunk.text, end="", flush=True)
    # --- 整合結束 ---

    print("\n\n--- 強制刷新追蹤數據 ---")
    tp.force_flush()

if __name__ == "__main__":
    asyncio.run(main())
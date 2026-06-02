from agent_framework.observability import configure_otel_providers

# Enable tracing and capture sensitive data (like tool arguments/results)
configure_otel_providers(enable_sensitive_data=True)


import asyncio
from datetime import datetime
from agent_framework.ollama import OllamaChatClient

# --- 1. SKILLS DEFINITION ---
def get_current_location() -> str:
    """Get the user's current city."""
    return "Hong Kong"

def get_current_date() -> str:
    """Get today's date in YYYY-MM-DD format."""
    return f"Today is {datetime.now().strftime('%Y-%m-%d')}."

def get_historical_weather(location: str, date: str) -> str:
    """Get weather for a specific city and date."""
    return f"The weather in {location} on {date} was Rainy, 19°C."

# --- 2. AGENT CONFIGURATION ---
SYSTEM_INSTRUCTIONS = """
You are a Microsoft Agent. Use the ReAct pattern: Thought -> Action -> Observation.
1. Call 'get_current_location' and 'get_current_date' first.
2. Use that info to call 'get_historical_weather'.
"""

async def run_maf_agent():
    # Initialize the Ollama Client
    # MAF defaults to http://localhost:11434
    # client = OllamaChatClient(model_id="qwen2.5:7b")
    client = OllamaChatClient(model_id="gemma4:e4b")

    # Create the agent using the .as_agent() factory method
    # This is the standard way in the latest MAF Python SDK
    agent = client.as_agent(
        name="WeatherAssistant",
        instructions=SYSTEM_INSTRUCTIONS,
        tools=[get_current_location, get_current_date, get_historical_weather]
    )

    print("--- Starting MAF Agent (Ollama Qwen2.5) ---\n")
    query = "我在這裡昨天天氣如何？"

    # In MAF, agent.run() returns an async generator for streaming
    async for chunk in agent.run(query, stream=True):
        if chunk.text:
            print(chunk.text, end="", flush=True)

if __name__ == "__main__":
    asyncio.run(run_maf_agent())
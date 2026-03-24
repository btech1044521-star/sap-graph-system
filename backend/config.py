import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the same directory as this file (backend/)
load_dotenv(Path(__file__).resolve().parent / ".env")

NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://1c15aba9.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "1c15aba9")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "h33zMV_gg-AbtLghxspJ4wOBXZfS9xMUpwKP2D8Ju2w")
DATA_DIR = os.getenv("DATA_DIR", "")

# OpenRouter LLM settings
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free")

# Error-handling / retry settings
MAX_CYPHER_RETRIES = int(os.getenv("MAX_CYPHER_RETRIES", "3"))
CYPHER_TIMEOUT = int(os.getenv("CYPHER_TIMEOUT", "30"))

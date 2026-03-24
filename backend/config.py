import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the same directory as this file (backend/)
load_dotenv(Path(__file__).resolve().parent / ".env")

NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://1c15aba9.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "1c15aba9")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "h33zMV_gg-AbtLghxspJ4wOBXZfS9xMUpwKP2D8Ju2w")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")  # "ollama" or "gemini"
DATA_DIR = os.getenv("DATA_DIR", "")

# Error-handling / retry settings
MAX_CYPHER_RETRIES = int(os.getenv("MAX_CYPHER_RETRIES", "3"))
CYPHER_TIMEOUT = int(os.getenv("CYPHER_TIMEOUT", "30"))

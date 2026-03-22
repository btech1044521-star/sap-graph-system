import os
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "sapgraph123")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DATA_DIR = os.getenv("DATA_DIR", "")

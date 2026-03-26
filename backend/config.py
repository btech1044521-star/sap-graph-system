import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Try multiple locations to find .env
_backend_dir = Path(__file__).resolve().parent
_env_candidates = [
    _backend_dir / ".env",                      # backend/.env (same dir as config.py)
    _backend_dir.parent / ".env",               # project root .env
    Path.cwd() / "backend" / ".env",            # cwd/backend/.env
    Path.cwd() / ".env",                        # cwd/.env
]

_loaded = False
for _env_path in _env_candidates:
    if _env_path.is_file():
        load_dotenv(_env_path, override=True)
        _loaded = True
        break

if not _loaded:
    load_dotenv()  # fallback: search up from cwd

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
DATA_DIR = os.getenv("DATA_DIR", "")

# OpenRouter LLM settings
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free")

# Groq (free tier — llama3-70b)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# Google Gemini (free tier)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# Error-handling / retry settings
MAX_CYPHER_RETRIES = int(os.getenv("MAX_CYPHER_RETRIES", "3"))
CYPHER_TIMEOUT = int(os.getenv("CYPHER_TIMEOUT", "30"))

# CORS: default to wildcard so deploys work out-of-the-box.
# Set CORS_ORIGINS env var to a comma-separated list to restrict.
_cors_raw = os.getenv("CORS_ORIGINS", "").strip()
if not _cors_raw or _cors_raw == "*":
    CORS_ORIGINS = ["*"]
else:
    CORS_ORIGINS = [o.strip() for o in _cors_raw.split(",") if o.strip()]

print(f"[config] CORS_ORIGINS={CORS_ORIGINS}")

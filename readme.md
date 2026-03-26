# SAP Graph System Deployment Guide

This repository can be deployed on Render or Railway as two services:
- Backend: FastAPI in `backend`
- Frontend: Vite app in `frontend`

## 1. Pre-deploy checklist

- Use Python 3.11 for backend deployments.
- Ensure Neo4j Aura credentials are ready.
- Ensure at least one LLM provider key is set:
  - `OPENROUTER_API_KEY`, or
  - `GROQ_API_KEY`, or
  - `GEMINI_API_KEY`

## 2. Deploy on Render (recommended)

Use the blueprint file at `render.yaml`.

### Steps

1. Push repository to GitHub.
2. In Render, select New + > Blueprint and connect this repo.
3. Render creates two services from `render.yaml`:
	- `sap-graph-backend`
	- `sap-graph-frontend`
4. Fill required env vars in Render dashboard:
	- Backend:
	  - `NEO4J_URI`
	  - `NEO4J_USER`
	  - `NEO4J_PASSWORD`
		 - `CORS_ORIGINS=https://<your-frontend-service>.onrender.com`
	  - one or more LLM keys/models
	- Frontend:
	  - `VITE_API_BASE_URL=https://<your-backend-service>.onrender.com/api`
5. Deploy both services.

### Health check

Backend health endpoint:
- `/api/health`

## 3. Deploy on Railway

### Backend service

1. New Project > Deploy from GitHub Repo.
2. Set service root directory to `backend`.
3. Railway start command can use `backend/Procfile`:
	- `web: uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Add backend env vars (same list as Render).
	- Include `CORS_ORIGINS=https://<your-frontend>.up.railway.app` when frontend is hosted separately.

### Frontend service

1. Add a second service from the same repo, root `frontend`.
2. Build command:
	- `npm ci && npm run build`
3. Start command:
	- `npm run start`
4. Set env var:
	- `VITE_API_BASE_URL=https://<your-backend>.up.railway.app/api`

## 4. Local vs production API base

The frontend now uses:
- `VITE_API_BASE_URL` when set
- otherwise `/api` for local proxy in Vite

File:
- `frontend/src/api.js`

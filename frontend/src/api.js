const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

async function apiFetch(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `API error ${res.status}`);
  }
  return res.json();
}

export async function fetchGraphOverview() {
  return apiFetch(`${API_BASE}/graph/overview`);
}

export async function fetchGraphSample(limit = 50) {
  return apiFetch(`${API_BASE}/graph/sample?limit=${limit}`);
}

export async function fetchNodeDetail(label, id) {
  return apiFetch(`${API_BASE}/graph/node/${encodeURIComponent(label)}/${encodeURIComponent(id)}`);
}

export async function expandNode(label, id) {
  return apiFetch(`${API_BASE}/graph/expand/${encodeURIComponent(label)}/${encodeURIComponent(id)}`);
}

export async function searchNodes(query, label = null) {
  let url = `${API_BASE}/graph/search?q=${encodeURIComponent(query)}`;
  if (label) url += `&label=${encodeURIComponent(label)}`;
  return apiFetch(url);
}

export async function sendChatMessage(message, history = []) {
  return apiFetch(`${API_BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history })
  });
}

export async function checkHealth() {
  return apiFetch(`${API_BASE}/health`);
}

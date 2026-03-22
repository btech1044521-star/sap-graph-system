const API_BASE = '/api';

export async function fetchGraphOverview() {
  const res = await fetch(`${API_BASE}/graph/overview`);
  return res.json();
}

export async function fetchGraphSample(limit = 50) {
  const res = await fetch(`${API_BASE}/graph/sample?limit=${limit}`);
  return res.json();
}

export async function fetchNodeDetail(label, id) {
  const res = await fetch(`${API_BASE}/graph/node/${encodeURIComponent(label)}/${encodeURIComponent(id)}`);
  return res.json();
}

export async function expandNode(label, id) {
  const res = await fetch(`${API_BASE}/graph/expand/${encodeURIComponent(label)}/${encodeURIComponent(id)}`);
  return res.json();
}

export async function searchNodes(query, label = null) {
  let url = `${API_BASE}/graph/search?q=${encodeURIComponent(query)}`;
  if (label) url += `&label=${encodeURIComponent(label)}`;
  const res = await fetch(url);
  return res.json();
}

export async function sendChatMessage(message, history = []) {
  const res = await fetch(`${API_BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history })
  });
  return res.json();
}

export async function checkHealth() {
  const res = await fetch(`${API_BASE}/health`);
  return res.json();
}

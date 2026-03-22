"""
FastAPI backend for SAP O2C Graph Query System.
Provides endpoints for graph exploration and LLM-powered natural language queries.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from database import run_cypher, Neo4jConnection
from llm_engine import query as llm_query

app = FastAPI(title="SAP O2C Graph Query System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Models ───────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []


class ChatResponse(BaseModel):
    answer: str
    cypher: str | None = None
    results: list[dict] = []
    guardrail: bool = False


class GraphRequest(BaseModel):
    nodeId: str | None = None
    nodeLabel: str | None = None
    limit: int = 100


# ─── Graph Exploration Endpoints ──────────────────────────

@app.get("/api/graph/overview")
def graph_overview():
    """Get high-level graph stats and a sample subgraph for initial visualization."""
    stats = run_cypher("""
        CALL {
            MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count
        }
        RETURN label, count ORDER BY count DESC
    """)
    rel_stats = run_cypher("""
        CALL {
            MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count
        }
        RETURN type, count ORDER BY count DESC
    """)
    return {"nodeStats": stats, "relStats": rel_stats}


@app.get("/api/graph/sample")
def graph_sample(limit: int = 50):
    """Get a sample subgraph showing the O2C flow."""
    result = run_cypher("""
        MATCH (c:Customer)-[:PLACED_ORDER]->(so:SalesOrder)
        WITH c, so LIMIT $limit
        OPTIONAL MATCH (so)-[:HAS_ITEM]->(si:SalesOrderItem)
        OPTIONAL MATCH (so)-[:FULFILLED_BY]->(di:DeliveryItem)<-[:HAS_ITEM]-(d:Delivery)
        OPTIONAL MATCH (so)-[:BILLED_IN]->(bi:BillingDocumentItem)<-[:HAS_ITEM]-(b:BillingDocument)
        OPTIONAL MATCH (si)-[:CONTAINS_PRODUCT]->(p:Product)
        RETURN c, so, si, di, d, bi, b, p
        LIMIT $limit
    """, {"limit": limit})

    nodes = {}
    edges = []

    for record in result:
        for key, val in record.items():
            if val is None:
                continue
            if isinstance(val, dict):
                # It's a node
                node_id = val.get("id")
                if node_id and node_id not in nodes:
                    label = key_to_label(key)
                    nodes[node_id] = {
                        "id": node_id,
                        "label": label,
                        "properties": val
                    }

    # Fetch relationships for these nodes
    node_ids = list(nodes.keys())
    if node_ids:
        rels = run_cypher("""
            MATCH (a)-[r]->(b)
            WHERE a.id IN $ids AND b.id IN $ids
            RETURN a.id AS source, b.id AS target, type(r) AS type
        """, {"ids": node_ids})
        edges = [{"source": r["source"], "target": r["target"], "type": r["type"]} for r in rels]

    return {"nodes": list(nodes.values()), "edges": edges}


@app.get("/api/graph/node/{node_label}/{node_id}")
def get_node(node_label: str, node_id: str):
    """Get a specific node and its immediate neighbors."""
    allowed_labels = {
        "Customer", "SalesOrder", "SalesOrderItem", "Delivery", "DeliveryItem",
        "BillingDocument", "BillingDocumentItem", "JournalEntry", "Payment",
        "Product", "Plant", "Address"
    }
    if node_label not in allowed_labels:
        raise HTTPException(status_code=400, detail="Invalid node label")

    # Get the node
    node_result = run_cypher(
        f"MATCH (n:{node_label} {{id: $id}}) RETURN n",
        {"id": node_id}
    )
    if not node_result:
        raise HTTPException(status_code=404, detail="Node not found")

    # Get neighbors
    neighbors = run_cypher(f"""
        MATCH (n:{node_label} {{id: $id}})-[r]-(m)
        RETURN m.id AS id, labels(m)[0] AS label, type(r) AS relType,
               CASE WHEN startNode(r) = n THEN 'outgoing' ELSE 'incoming' END AS direction,
               properties(m) AS properties
        LIMIT 50
    """, {"id": node_id})

    return {
        "node": {"id": node_id, "label": node_label, "properties": node_result[0]["n"]},
        "neighbors": neighbors
    }


@app.get("/api/graph/expand/{node_label}/{node_id}")
def expand_node(node_label: str, node_id: str):
    """Expand a node to show its relationships for graph visualization."""
    allowed_labels = {
        "Customer", "SalesOrder", "SalesOrderItem", "Delivery", "DeliveryItem",
        "BillingDocument", "BillingDocumentItem", "JournalEntry", "Payment",
        "Product", "Plant", "Address"
    }
    if node_label not in allowed_labels:
        raise HTTPException(status_code=400, detail="Invalid node label")

    result = run_cypher(f"""
        MATCH (n:{node_label} {{id: $id}})-[r]-(m)
        RETURN n.id AS sourceId, labels(n)[0] AS sourceLabel, properties(n) AS sourceProps,
               m.id AS targetId, labels(m)[0] AS targetLabel, properties(m) AS targetProps,
               type(r) AS relType,
               CASE WHEN startNode(r) = n THEN 'outgoing' ELSE 'incoming' END AS direction
        LIMIT 50
    """, {"id": node_id})

    nodes = {}
    edges = []

    for r in result:
        if r["sourceId"] not in nodes:
            nodes[r["sourceId"]] = {
                "id": r["sourceId"],
                "label": r["sourceLabel"],
                "properties": r["sourceProps"]
            }
        if r["targetId"] not in nodes:
            nodes[r["targetId"]] = {
                "id": r["targetId"],
                "label": r["targetLabel"],
                "properties": r["targetProps"]
            }
        if r["direction"] == "outgoing":
            edges.append({"source": r["sourceId"], "target": r["targetId"], "type": r["relType"]})
        else:
            edges.append({"source": r["targetId"], "target": r["sourceId"], "type": r["relType"]})

    return {"nodes": list(nodes.values()), "edges": edges}


@app.get("/api/graph/search")
def search_nodes(q: str, label: str = None, limit: int = 20):
    """Search nodes by ID or property values."""
    if label:
        allowed_labels = {
            "Customer", "SalesOrder", "SalesOrderItem", "Delivery", "DeliveryItem",
            "BillingDocument", "BillingDocumentItem", "JournalEntry", "Payment",
            "Product", "Plant", "Address"
        }
        if label not in allowed_labels:
            raise HTTPException(status_code=400, detail="Invalid node label")
        results = run_cypher(f"""
            MATCH (n:{label})
            WHERE n.id CONTAINS $q
               OR n.name CONTAINS $q
               OR n.description CONTAINS $q
               OR n.shortName CONTAINS $q
            RETURN n.id AS id, labels(n)[0] AS label, properties(n) AS properties
            LIMIT $limit
        """, {"q": q, "limit": limit})
    else:
        results = run_cypher("""
            MATCH (n)
            WHERE n.id CONTAINS $q
               OR n.name CONTAINS $q
               OR n.description CONTAINS $q
               OR n.shortName CONTAINS $q
            RETURN n.id AS id, labels(n)[0] AS label, properties(n) AS properties
            LIMIT $limit
        """, {"q": q, "limit": limit})

    return {"results": results}


# ─── Chat / Query Endpoint ────────────────────────────────

@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """Natural language query interface powered by LLM."""
    result = llm_query(req.message, req.history)
    return ChatResponse(**result)


# ─── Health ───────────────────────────────────────────────

@app.get("/api/health")
def health():
    try:
        run_cypher("RETURN 1")
        return {"status": "ok", "neo4j": "connected"}
    except Exception as e:
        return {"status": "degraded", "neo4j": str(e)}


@app.on_event("shutdown")
def shutdown():
    Neo4jConnection.close()


# ─── Helpers ──────────────────────────────────────────────

def key_to_label(key: str) -> str:
    mapping = {
        "c": "Customer", "so": "SalesOrder", "si": "SalesOrderItem",
        "d": "Delivery", "di": "DeliveryItem", "b": "BillingDocument",
        "bi": "BillingDocumentItem", "j": "JournalEntry", "pay": "Payment",
        "p": "Product", "pl": "Plant", "a": "Address"
    }
    return mapping.get(key, "Unknown")

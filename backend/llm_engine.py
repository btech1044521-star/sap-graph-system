"""
LLM-powered query engine: Translates natural language to Cypher queries
using Ollama (local) or Google Gemini, executes them against Neo4j,
and returns natural language answers.

Includes retry logic, syntax validation, auto-correction, dry-run testing,
fallback simplification, and query metrics tracking.
"""

import httpx
import re
import time
import logging
from typing import Optional

from config import (
    GEMINI_API_KEY, OLLAMA_BASE_URL, OLLAMA_MODEL, LLM_PROVIDER,
    MAX_CYPHER_RETRIES, CYPHER_TIMEOUT,
)
from database import run_cypher

logger = logging.getLogger(__name__)

# Lazy import Gemini only if needed
_genai = None
def _get_genai():
    global _genai
    if _genai is None:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        _genai = genai
    return _genai


# ============================================================================
# SCHEMA & PROMPTS (unchanged)
# ============================================================================

GRAPH_SCHEMA = """
Neo4j Graph Schema for SAP Order-to-Cash system:

NODE LABELS AND PROPERTIES:
- Customer: id, name, shortName, category, grouping, language, firstName, lastName, orgName, industry, isBlocked, creationDate
- SalesOrder: id, type, salesOrg, distributionChannel, totalNetAmount (float), currency, creationDate, deliveryStatus, billingStatus, requestedDeliveryDate, paymentTerms, soldToParty
- SalesOrderItem: id, salesOrder, itemNumber, category, material, requestedQuantity (float), quantityUnit, netAmount (float), currency, plant, storageLocation, confirmedDeliveryDate, confirmedQuantity (float)
- Delivery: id, goodsMovementDate, creationDate, shippingPoint, pickingStatus, goodsMovementStatus, incompletionStatus
- DeliveryItem: id, deliveryDocument, itemNumber, quantity (float), quantityUnit, plant, storageLocation, referenceSdDocument, referenceSdDocumentItem
- BillingDocument: id, type, creationDate, billingDate, isCancelled, cancelledDocument, totalNetAmount (float), currency, companyCode, fiscalYear, accountingDocument, soldToParty
- BillingDocumentItem: id, billingDocument, itemNumber, material, quantity (float), quantityUnit, netAmount (float), currency, referenceSdDocument, referenceSdDocumentItem
- JournalEntry: id, companyCode, fiscalYear, accountingDocument, documentType, itemNumber, glAccount, amount (float), currency, localAmount (float), localCurrency, postingDate, documentDate, customer, profitCenter, clearingDate, clearingDocument, referenceDocument
- Payment: id, companyCode, fiscalYear, accountingDocument, itemNumber, amount (float), currency, localAmount (float), localCurrency, postingDate, documentDate, customer, glAccount, profitCenter, clearingDate, clearingDocument, invoiceReference, salesDocument
- Product: id, type, group, baseUnit, grossWeight, netWeight, weightUnit, division, creationDate, description
- Plant: id, name, salesOrganization, distributionChannel, language
- Address: id, city, country, postalCode, region, street, addressId

RELATIONSHIPS:
- (Customer)-[:PLACED_ORDER]->(SalesOrder)
- (Customer)-[:HAS_ADDRESS]->(Address)
- (Customer)-[:BILLED_TO]->(BillingDocument)
- (SalesOrder)-[:HAS_ITEM]->(SalesOrderItem)
- (SalesOrder)-[:FULFILLED_BY]->(DeliveryItem)
- (SalesOrder)-[:BILLED_IN]->(BillingDocumentItem)
- (SalesOrderItem)-[:CONTAINS_PRODUCT]->(Product)
- (SalesOrderItem)-[:FULFILLED_FROM_PLANT]->(Plant)
- (Delivery)-[:HAS_ITEM]->(DeliveryItem)
- (DeliveryItem)-[:SHIPPED_FROM]->(Plant)
- (BillingDocument)-[:HAS_ITEM]->(BillingDocumentItem)
- (BillingDocument)-[:GENERATES_JOURNAL_ENTRY]->(JournalEntry)
- (BillingDocumentItem)-[:BILLS_PRODUCT]->(Product)
- (JournalEntry)-[:POSTED_FOR]->(Customer)
- (Payment)-[:PAID_BY]->(Customer)
- (Payment)-[:PAYS_FOR]->(SalesOrder)
- (Product)-[:PRODUCED_AT]->(Plant)

KEY O2C FLOW: Customer -PLACED_ORDER-> SalesOrder -FULFILLED_BY-> DeliveryItem <-HAS_ITEM- Delivery
              SalesOrder -BILLED_IN-> BillingDocumentItem <-HAS_ITEM- BillingDocument -GENERATES_JOURNAL_ENTRY-> JournalEntry
              Payment -PAYS_FOR-> SalesOrder
"""

SYSTEM_PROMPT = f"""You are a query assistant for an SAP Order-to-Cash graph database stored in Neo4j.

{GRAPH_SCHEMA}

YOUR TASK:
Given a user's natural language question, generate a valid Cypher query to answer it.

RULES:
1. ONLY generate Cypher queries relevant to the SAP Order-to-Cash dataset described above.
2. If the question is NOT related to this dataset (e.g., general knowledge, creative writing, unrelated topics), respond with EXACTLY: GUARDRAIL: This system is designed to answer questions related to the SAP Order-to-Cash dataset only.
3. Return ONLY the Cypher query, no explanation. Do NOT wrap in markdown code blocks.
4. Use LIMIT to cap results (max 25 rows) unless the user specifies otherwise.
5. For aggregate queries, always include relevant labels/names alongside counts.
6. When tracing flows, traverse the full O2C path.
7. For "broken/incomplete flows", check for missing relationships.
8. Property values are strings unless noted as (float) in schema.
9. Use toFloat() when comparing numeric string properties.
10. Node IDs are stored in the `id` property, not Neo4j internal IDs.
"""

RETRY_ADDENDUM = """
IMPORTANT - the previous Cypher query failed with this error:
{error}

Fix the query. Common mistakes to avoid:
- Unbalanced parentheses / brackets
- MATCH without RETURN
- Wrong node label or relationship type (check schema above)
- Missing LIMIT clause
- Using Neo4j internal id() instead of the `id` property
- Incorrect property names (check schema carefully)

Generate a corrected Cypher query for the original question: {question}
"""

ANSWER_PROMPT = """You are a data analyst for an SAP Order-to-Cash system.

Given the user's question and the query results from the graph database, provide a clear, concise, data-backed answer in natural language.

RULES:
1. Ground your answer ONLY in the provided data. Do not hallucinate or add information not in the results.
2. Format numbers clearly (e.g., currency with 2 decimal places).
3. If results are empty, say so clearly and suggest why.
4. Keep the answer concise but complete.
5. If the data contains IDs, mention them for traceability.
6. Use bullet points or tables for multiple results when appropriate.
"""


# ============================================================================
# CYPHER VALIDATION & AUTO-CORRECTION
# ============================================================================

VALID_LABELS = {
    "Customer", "SalesOrder", "SalesOrderItem", "Delivery", "DeliveryItem",
    "BillingDocument", "BillingDocumentItem", "JournalEntry", "Payment",
    "Product", "Plant", "Address",
}

VALID_REL_TYPES = {
    "PLACED_ORDER", "HAS_ADDRESS", "BILLED_TO", "HAS_ITEM", "FULFILLED_BY",
    "BILLED_IN", "CONTAINS_PRODUCT", "FULFILLED_FROM_PLANT", "SHIPPED_FROM",
    "GENERATES_JOURNAL_ENTRY", "BILLS_PRODUCT", "POSTED_FOR", "PAID_BY",
    "PAYS_FOR", "PRODUCED_AT",
}


def _validate_syntax(cypher: str) -> tuple[bool, list[str]]:
    """Pre-execution syntax checks."""
    errors = []
    if cypher.count('(') != cypher.count(')'):
        errors.append("Unbalanced parentheses")
    if cypher.count('[') != cypher.count(']'):
        errors.append("Unbalanced brackets")
    if cypher.count('{') != cypher.count('}'):
        errors.append("Unbalanced braces")

    has_match = bool(re.search(r'\bMATCH\b', cypher, re.IGNORECASE))
    has_return = bool(re.search(r'\bRETURN\b', cypher, re.IGNORECASE))
    if has_match and not has_return:
        errors.append("MATCH without RETURN")
    if not has_match and not re.search(r'\bCALL\b', cypher, re.IGNORECASE):
        errors.append("No MATCH or CALL statement")

    return len(errors) == 0, errors


def _auto_correct(cypher: str) -> str:
    """Fix common LLM-generated Cypher mistakes."""
    corrected = cypher

    # Ensure LIMIT exists for read queries
    if (re.search(r'\bRETURN\b', corrected, re.IGNORECASE)
            and not re.search(r'\bLIMIT\b', corrected, re.IGNORECASE)):
        corrected = corrected.rstrip().rstrip(';') + " LIMIT 25"

    # Fix stray semicolons (Neo4j driver dislikes trailing ones)
    corrected = corrected.rstrip(';').strip()

    return corrected


def _clean_cypher(raw: str) -> str:
    """Strip markdown fences, backticks, and normalise whitespace."""
    cleaned = raw.replace("```cypher", "").replace("```", "").strip()
    cleaned = cleaned.strip('`').strip()
    # collapse multi-space but preserve newlines for readability
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    return cleaned


def _dry_run(cypher: str) -> tuple[bool, str]:
    """EXPLAIN without executing — catches syntax errors cheaply."""
    try:
        run_cypher(f"EXPLAIN {cypher}", timeout=8)
        return True, ""
    except Exception as e:
        return False, str(e)


# ============================================================================
# FALLBACK SIMPLIFICATION STRATEGIES
# ============================================================================

def _strip_optional_matches(cypher: str) -> str:
    """Remove OPTIONAL MATCH clauses that may cause planning issues."""
    return re.sub(
        r'OPTIONAL\s+MATCH\s+.+?(?=\bMATCH\b|\bWHERE\b|\bRETURN\b|\bWITH\b|$)',
        '', cypher, flags=re.IGNORECASE | re.DOTALL
    ).strip()


def _strip_with_clauses(cypher: str) -> str:
    """Remove intermediate WITH piping that can confuse the planner."""
    simplified = re.sub(
        r'\bWITH\b\s+.+?(?=\bMATCH\b|\bRETURN\b)',
        '', cypher, flags=re.IGNORECASE | re.DOTALL
    ).strip()
    return simplified


def _cap_variable_length(cypher: str) -> str:
    """Replace unbounded variable-length paths *.. with *1..3."""
    return re.sub(r'\*\.\.', '*1..3', cypher)


_FALLBACK_STRATEGIES = [
    _strip_optional_matches,
    _strip_with_clauses,
    _cap_variable_length,
]


# ============================================================================
# LLM CALLS (unchanged core logic)
# ============================================================================

def is_guardrail_response(text: str) -> bool:
    return text.strip().startswith("GUARDRAIL:")


def _call_ollama(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    parts = [f"[SYSTEM]\n{system}\n[/SYSTEM]\n"]
    if conversation_history:
        for msg in conversation_history[-6:]:
            role = msg.get("role", "user").upper()
            parts.append(f"[{role}]\n{msg['content']}\n")
    parts.append(f"[USER]\n{prompt}\n[/USER]\n[ASSISTANT]\n")
    full_prompt = "\n".join(parts)

    with httpx.Client(timeout=120.0) as client:
        resp = client.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": full_prompt, "stream": False},
        )
        resp.raise_for_status()
        return resp.json()["response"].strip()


def _call_gemini(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    genai = _get_genai()
    model = genai.GenerativeModel("gemini-2.0-flash")
    messages = [
        {"role": "user", "parts": [system]},
        {"role": "model", "parts": ["Understood. I will generate Cypher queries for SAP O2C data or return a GUARDRAIL message for off-topic questions."]},
    ]
    if conversation_history:
        for msg in conversation_history[-6:]:
            messages.append({"role": msg["role"], "parts": [msg["content"]]})
    messages.append({"role": "user", "parts": [prompt]})
    response = model.generate_content(messages)
    return response.text.strip()


def _call_llm(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    if LLM_PROVIDER == "gemini":
        return _call_gemini(system, prompt, conversation_history)
    return _call_ollama(system, prompt, conversation_history)


def generate_answer(user_query: str, cypher_query: str, results: list[dict]) -> str:
    results_str = str(results[:50])
    prompt = f"""User Question: {user_query}

Cypher Query Executed: {cypher_query}

Query Results (up to 50 rows):
{results_str}

Provide a natural language answer:"""
    return _call_llm(ANSWER_PROMPT, prompt)


# ============================================================================
# QUERY METRICS
# ============================================================================

class QueryMetrics:
    """In-memory metrics for monitoring error rate."""

    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.guardrail = 0
        self.retries_used: list[int] = []
        self.error_types: dict[str, int] = {}

    def record_success(self, attempts: int):
        self.total += 1
        self.success += 1
        self.retries_used.append(attempts)

    def record_failure(self, error_type: str):
        self.total += 1
        self.failed += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1

    def record_guardrail(self):
        self.total += 1
        self.guardrail += 1

    @property
    def success_rate(self) -> float:
        return self.success / self.total if self.total else 1.0

    @property
    def avg_retries(self) -> float:
        return sum(self.retries_used) / len(self.retries_used) if self.retries_used else 0.0

    def report(self) -> dict:
        return {
            "total_queries": self.total,
            "successful": self.success,
            "failed": self.failed,
            "guardrail": self.guardrail,
            "success_rate": f"{self.success_rate * 100:.2f}%",
            "avg_retries": f"{self.avg_retries:.2f}",
            "error_distribution": dict(self.error_types),
        }


metrics = QueryMetrics()


def get_metrics() -> dict:
    return metrics.report()


# ============================================================================
# MAIN QUERY FUNCTION  — with retry, validation, correction, fallback
# ============================================================================

def query(user_query: str, conversation_history: list[dict] = None) -> dict:
    """
    End-to-end pipeline:
      1. LLM generates Cypher
      2. Validate / auto-correct syntax
      3. EXPLAIN dry-run
      4. Execute; on failure → retry with error context or fallback simplification
      5. LLM formats answer
    """
    last_error = ""
    cypher_clean = None

    for attempt in range(1, MAX_CYPHER_RETRIES + 1):
        try:
            # ── Step 1: generate Cypher ──────────────────────────
            if attempt == 1:
                raw = _call_llm(
                    SYSTEM_PROMPT,
                    f"Generate a Cypher query for: {user_query}",
                    conversation_history,
                )
            else:
                # Retry: include the previous error so the LLM can self-correct
                retry_prompt = RETRY_ADDENDUM.format(error=last_error, question=user_query)
                raw = _call_llm(SYSTEM_PROMPT, retry_prompt, conversation_history)

            # ── Guardrail check ──────────────────────────────────
            if is_guardrail_response(raw):
                metrics.record_guardrail()
                return {
                    "answer": raw.replace("GUARDRAIL: ", ""),
                    "cypher": None,
                    "results": [],
                    "guardrail": True,
                }

            # ── Step 2: clean & validate ─────────────────────────
            cypher_clean = _clean_cypher(raw)
            cypher_clean = _auto_correct(cypher_clean)

            valid, syntax_errors = _validate_syntax(cypher_clean)
            if not valid:
                last_error = f"Syntax validation: {'; '.join(syntax_errors)}"
                logger.warning("Attempt %d syntax fail: %s", attempt, last_error)
                continue  # next retry

            # ── Step 3: dry-run (EXPLAIN) ────────────────────────
            ok, explain_err = _dry_run(cypher_clean)
            if not ok:
                last_error = explain_err
                logger.warning("Attempt %d dry-run fail: %s", attempt, explain_err)
                # Try fallback simplifications before burning a retry
                for strategy in _FALLBACK_STRATEGIES:
                    candidate = strategy(cypher_clean)
                    if candidate == cypher_clean:
                        continue
                    candidate = _auto_correct(candidate)
                    fb_ok, _ = _dry_run(candidate)
                    if fb_ok:
                        cypher_clean = candidate
                        ok = True
                        logger.info("Fallback %s fixed the query", strategy.__name__)
                        break
                if not ok:
                    continue  # next retry

            # ── Step 4: execute ──────────────────────────────────
            results = run_cypher(cypher_clean, timeout=CYPHER_TIMEOUT)

            # ── Step 5: generate answer ──────────────────────────
            answer = generate_answer(user_query, cypher_clean, results)
            metrics.record_success(attempt)
            return {
                "answer": answer,
                "cypher": cypher_clean,
                "results": results[:25],
                "guardrail": False,
            }

        except Exception as exc:
            last_error = str(exc)
            logger.error("Attempt %d exception: %s", attempt, last_error)
            # brief back-off before retry
            if attempt < MAX_CYPHER_RETRIES:
                time.sleep(min(attempt, 3))

    # All retries exhausted
    error_kind = _classify_error(last_error)
    metrics.record_failure(error_kind)
    return {
        "answer": (
            f"I was unable to answer after {MAX_CYPHER_RETRIES} attempts. "
            f"Last issue: {last_error}\n\nPlease try rephrasing your question."
        ),
        "cypher": cypher_clean,
        "results": [],
        "guardrail": False,
        "error": True,
    }


def _classify_error(msg: str) -> str:
    """Bucket errors for metrics."""
    m = msg.lower()
    if "syntax" in m:
        return "syntax_error"
    if "property" in m or "not found" in m:
        return "schema_mismatch"
    if "timeout" in m or "timed out" in m:
        return "timeout"
    if "connection" in m or "refused" in m:
        return "connection"
    return "unknown"

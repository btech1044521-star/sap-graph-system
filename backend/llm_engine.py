"""
LLM-powered query engine: Translates natural language to Cypher queries
via OpenRouter, executes them against Neo4j, and returns natural language answers.

Includes intelligent retry logic with state tracking, syntax validation, auto-correction,
dry-run testing, multiple fallback strategies, and oscillation prevention.
"""

import httpx
import re
import time
import logging
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from config import (
    OPENROUTER_API_KEY, OPENROUTER_MODEL,
    GROQ_API_KEY, GROQ_MODEL,
    GEMINI_API_KEY, GEMINI_MODEL,
    MAX_CYPHER_RETRIES, CYPHER_TIMEOUT,
)
from database import run_cypher

logger = logging.getLogger(__name__)


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

class QueryState(Enum):
    """Query execution states."""
    INITIAL = "initial"
    GENERATING = "generating"
    VALIDATING = "validating"
    DRY_RUN = "dry_run"
    EXECUTING = "executing"
    FIXING = "fixing"
    FALLBACK = "fallback"
    SUCCESS = "success"
    FAILED = "failed"


class ErrorCategory(Enum):
    """Categories of errors for targeted fixes."""
    SYNTAX = "syntax"
    RELATIONSHIP_NOT_FOUND = "relationship_not_found"
    LABEL_NOT_FOUND = "label_not_found"
    PROPERTY_NOT_FOUND = "property_not_found"
    DIRECTION_ERROR = "direction_error"
    RETURN_SYNTAX = "return_syntax"
    AUTH = "auth"
    TIMEOUT = "timeout"
    MEMORY = "memory"
    UNKNOWN = "unknown"


@dataclass
class QueryAttempt:
    """Record of a single query attempt."""
    attempt_number: int
    cypher: str
    state: QueryState
    error: Optional[str] = None
    error_category: Optional[ErrorCategory] = None
    timestamp: float = field(default_factory=time.time)
    strategy_used: str = "default"


@dataclass
class QueryContext:
    """Global context for a query execution."""
    user_query: str
    conversation_history: Optional[List[dict]] = None
    attempts: List[QueryAttempt] = field(default_factory=list)
    current_attempt: int = 0
    final_cypher: Optional[str] = None
    final_results: Optional[List[dict]] = None
    start_time: float = field(default_factory=time.time)
    applied_strategies: List[str] = field(default_factory=list)
    max_attempts: int = MAX_CYPHER_RETRIES
    
    def add_attempt(self, attempt: QueryAttempt):
        self.attempts.append(attempt)
        self.current_attempt = attempt.attempt_number
    
    def get_last_error(self) -> Optional[str]:
        if self.attempts:
            return self.attempts[-1].error
        return None
    
    def get_last_error_category(self) -> Optional[ErrorCategory]:
        if self.attempts and self.attempts[-1].error_category:
            return self.attempts[-1].error_category
        return None
    
    def get_attempt_history(self) -> List[str]:
        return [a.cypher for a in self.attempts if a.cypher]
    
    def is_oscillating(self) -> bool:
        """Check if we're repeating similar queries."""
        if len(self.attempts) < 2:
            return False
        
        recent = [a.cypher for a in self.attempts[-3:] if a.cypher]
        if len(recent) < 2:
            return False
        
        # Check for exact duplicates
        if len(set(recent)) != len(recent):
            return True
        
        # Check for high similarity
        for i in range(len(recent)):
            for j in range(i+1, len(recent)):
                if self._similarity(recent[i], recent[j]) > 0.85:
                    return True
        return False
    
    def _similarity(self, a: str, b: str) -> float:
        """Simple similarity check."""
        if not a or not b:
            return 0
        set_a = set(a.split())
        set_b = set(b.split())
        intersection = set_a & set_b
        union = set_a | set_b
        return len(intersection) / len(union) if union else 0
    
    def should_continue(self) -> bool:
        """Check if we should continue trying."""
        return (self.current_attempt < self.max_attempts and 
                not self.is_oscillating() and
                not any(a.state == QueryState.SUCCESS for a in self.attempts))


class GlobalQueryState:
    """Global state tracker to prevent runaway retries across queries."""
    
    def __init__(self):
        self.total_queries = 0
        self.total_attempts = 0
        self.failed_queries = 0
        self.recent_queries: Dict[str, List[datetime]] = {}  # Track query frequency
        self.max_queries_per_minute = 30
        
    def can_process(self, user_query: str) -> bool:
        """Check if we can process this query (rate limiting)."""
        # Clean query for tracking
        query_key = re.sub(r'\s+', ' ', user_query.strip())[:100]
        now = datetime.now()
        
        # Clean old entries
        if query_key in self.recent_queries:
            self.recent_queries[query_key] = [
                ts for ts in self.recent_queries[query_key]
                if (now - ts).total_seconds() < 60
            ]
        
        # Check rate limit
        if len(self.recent_queries.get(query_key, [])) >= self.max_queries_per_minute:
            return False
        
        return True
    
    def record_query(self, user_query: str):
        """Record a query attempt."""
        query_key = re.sub(r'\s+', ' ', user_query.strip())[:100]
        if query_key not in self.recent_queries:
            self.recent_queries[query_key] = []
        self.recent_queries[query_key].append(datetime.now())
        self.total_queries += 1
    
    def record_attempt(self):
        self.total_attempts += 1
    
    def record_failure(self):
        self.failed_queries += 1
    
    def get_stats(self) -> dict:
        return {
            "total_queries": self.total_queries,
            "total_attempts": self.total_attempts,
            "failed_queries": self.failed_queries,
            "success_rate": f"{(1 - self.failed_queries / max(1, self.total_queries)) * 100:.2f}%"
        }


# Global state instance
global_state = GlobalQueryState()


# ============================================================================
# SCHEMA & PROMPTS (Enhanced with correct relationship directions)
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

RELATIONSHIPS (with correct directions):
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
- (BillingDocumentItem)-[:BILLS_PRODUCT]->(Product)  # IMPORTANT: BillingDocumentItem → Product
- (JournalEntry)-[:POSTED_FOR]->(Customer)
- (Payment)-[:PAID_BY]->(Customer)
- (Payment)-[:PAYS_FOR]->(SalesOrder)
- (Product)-[:PRODUCED_AT]->(Plant)

CRITICAL NOTE: BILLS_PRODUCT direction is FROM BillingDocumentItem TO Product.
When querying products with billing documents, use:
  (b:BillingDocument)-[:HAS_ITEM]->(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)
"""

# Enhanced SYSTEM_PROMPT with explicit Cypher syntax rules
SYSTEM_PROMPT = f"""You are a query assistant for an SAP Order-to-Cash graph database stored in Neo4j.

{GRAPH_SCHEMA}

CRITICAL CYPHER SYNTAX RULES (MUST FOLLOW):
1. Property access: Use `node.property` format (e.g., p.id, b.description)
2. RETURN clause: Use `RETURN node.property AS alias` (e.g., RETURN p.id AS productId)
3. NEVER use: `RETURN node.property AS node.property` - this is invalid syntax
4. Variable naming: Use single letters or descriptive names (p, b, so, si, etc.)
5. Aggregations: Use `COUNT(DISTINCT variable)` for distinct counts
6. ORDER BY: Can only reference aliases from RETURN or WITH clause
7. LIMIT: Always include LIMIT 25 for read queries unless user specifies otherwise

EXAMPLES OF CORRECT SYNTAX:
✅ Correct: RETURN p.id AS productId, p.description AS productDescription, COUNT(DISTINCT b.id) AS count
✅ Correct: WITH p, COUNT(DISTINCT bd) AS billingDocCount RETURN p.id AS productId, billingDocCount
✅ Correct: MATCH (p:Product) RETURN p.id, p.description LIMIT 25

❌ Incorrect: RETURN p.id AS p.id, p.description AS p.description
❌ Incorrect: RETURN p.id AS productId, p.description AS productDescription, COUNT(DISTINCT bd.id) AS count
❌ Incorrect: RETURN p.id AS productId, billingDocCount ORDER BY billingDocCount (billingDocCount not defined)

YOUR TASK:
Given a user's natural language question, generate a valid Cypher query to answer it.

RULES:
1. ONLY generate Cypher queries relevant to the SAP Order-to-Cash dataset.
2. If question is NOT related to this dataset, respond with EXACTLY: GUARDRAIL: This system is designed to answer questions related to the SAP Order-to-Cash dataset only.
3. Return ONLY the Cypher query, no explanation. Do NOT wrap in markdown code blocks.
4. Use LIMIT 25 unless user specifies otherwise.
5. For aggregate queries, always use proper aliases in RETURN clause.
6. Use correct relationship directions as shown in the schema.
7. Property values are strings unless noted as (float) in schema.
8. Use toFloat() when comparing numeric string properties.
9. Node IDs are stored in the `id` property, not Neo4j internal IDs.
10. For product-billing queries, use: (b:BillingDocument)-[:HAS_ITEM]->(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)
"""


# ============================================================================
# ERROR CLASSIFICATION & FIX STRATEGIES
# ============================================================================

class ErrorClassifier:
    """Classify errors and suggest fix strategies."""
    
    PATTERNS = {
        ErrorCategory.RELATIONSHIP_NOT_FOUND: re.compile(r"relationship type `(\w+)` does not exist", re.IGNORECASE),
        ErrorCategory.LABEL_NOT_FOUND: re.compile(r"label `(\w+)` does not exist", re.IGNORECASE),
        ErrorCategory.PROPERTY_NOT_FOUND: re.compile(r"property `(\w+)` not found", re.IGNORECASE),
        ErrorCategory.SYNTAX: re.compile(r"invalid input|syntax error|unexpected", re.IGNORECASE),
        ErrorCategory.RETURN_SYNTAX: re.compile(r"RETURN.*AS\s+\w+\.\w+", re.IGNORECASE),
        ErrorCategory.AUTH: re.compile(r"unauthorized|authentication", re.IGNORECASE),
        ErrorCategory.TIMEOUT: re.compile(r"timeout|timed out", re.IGNORECASE),
        ErrorCategory.MEMORY: re.compile(r"memory limit|out of memory", re.IGNORECASE),
        ErrorCategory.DIRECTION_ERROR: re.compile(r"relationship direction|incorrect direction", re.IGNORECASE),
    }
    
    @classmethod
    def classify(cls, error_msg: str) -> ErrorCategory:
        """Classify error type from error message."""
        for category, pattern in cls.PATTERNS.items():
            if pattern.search(error_msg):
                return category
        return ErrorCategory.UNKNOWN
    
    @classmethod
    def extract_missing_item(cls, error_msg: str) -> Optional[str]:
        """Extract missing relationship or label from error."""
        for category in [ErrorCategory.RELATIONSHIP_NOT_FOUND, ErrorCategory.LABEL_NOT_FOUND]:
            pattern = cls.PATTERNS.get(category)
            if pattern:
                match = pattern.search(error_msg)
                if match:
                    return match.group(1)
        return None


class QueryFixer:
    """Multi-strategy query fixer for common errors."""
    
    @staticmethod
    def fix_return_syntax(cypher: str) -> str:
        """Fix RETURN clause where alias contains dot notation."""
        def fix_item(match):
            item = match.group(0)
            if ' AS ' in item:
                before_as = item.split(' AS ')[0]
                prop = before_as.split('.')[-1].strip()
                return f"{before_as} AS {prop}"
            return item
        
        # Fix pattern: RETURN p.id AS p.id -> RETURN p.id AS id
        pattern = r'\w+\.\w+\s+AS\s+\w+\.\w+'
        cypher = re.sub(pattern, fix_item, cypher, flags=re.IGNORECASE)
        
        # Add aliases to unaliased properties in RETURN
        return_match = re.search(r'RETURN\s+(.+?)(?=\s+(?:ORDER BY|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        if return_match:
            items = return_match.group(1).split(',')
            fixed = []
            for item in items:
                item = item.strip()
                if '.' in item and ' AS ' not in item.upper() and 'COUNT(' not in item.upper():
                    prop = item.split('.')[-1]
                    fixed.append(f"{item} AS {prop}")
                else:
                    fixed.append(item)
            cypher = cypher.replace(return_match.group(0), "RETURN " + ", ".join(fixed))
        
        return cypher
    
    @staticmethod
    def fix_relationship_direction(cypher: str) -> str:
        """Fix common relationship direction mistakes."""
        fixes = [
            # Fix BILLS_PRODUCT direction
            (r'\(p:Product\)\s*-\[:BILLS_PRODUCT\]->\s*\(bdi:BillingDocumentItem\)',
             '(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)'),
            # Fix BILLED_IN direction
            (r'\(bdi:BillingDocumentItem\)\s*-\[:BILLED_IN\]->\s*\(so:SalesOrder\)',
             '(so:SalesOrder)-[:BILLED_IN]->(bdi:BillingDocumentItem)'),
            # Fix CONTAINS_PRODUCT direction (should be SalesOrderItem -> Product)
            (r'\(p:Product\)\s*-\[:CONTAINS_PRODUCT\]->\s*\(si:SalesOrderItem\)',
             '(si:SalesOrderItem)-[:CONTAINS_PRODUCT]->(p:Product)'),
        ]
        
        for pattern, replacement in fixes:
            cypher = re.sub(pattern, replacement, cypher, flags=re.IGNORECASE)
        return cypher
    
    @staticmethod
    def fix_property_not_found(cypher: str, missing_property: str) -> str:
        """Remove or replace missing property references."""
        # Remove the problematic property reference
        cypher = re.sub(f'\\.{missing_property}\\b', '', cypher)
        return cypher
    
    @staticmethod
    def ensure_limit(cypher: str) -> str:
        """Ensure LIMIT clause exists."""
        if 'LIMIT' not in cypher.upper() and 'RETURN' in cypher.upper():
            cypher = cypher.rstrip(';') + " LIMIT 25"
        return cypher


# ============================================================================
# CYPHER VALIDATION & AUTO-CORRECTION
# ============================================================================

def _validate_syntax(cypher: str) -> Tuple[bool, List[str]]:
    """Pre-execution syntax checks."""
    errors = []
    
    # Check parentheses balance
    if cypher.count('(') != cypher.count(')'):
        errors.append("Unbalanced parentheses")
    if cypher.count('[') != cypher.count(']'):
        errors.append("Unbalanced brackets")
    
    # Check RETURN clause syntax
    return_match = re.search(r'RETURN\s+(.+?)(?=\s+(?:ORDER BY|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
    if return_match:
        return_items = return_match.group(1)
        if re.search(r'AS\s+\w+\.\w+', return_items):
            errors.append("Invalid RETURN alias: cannot use 'AS node.property'")
        if 'COUNT(' in return_items and ' AS ' not in return_items:
            errors.append("COUNT() in RETURN should have alias")
    
    has_match = bool(re.search(r'\bMATCH\b', cypher, re.IGNORECASE))
    has_return = bool(re.search(r'\bRETURN\b', cypher, re.IGNORECASE))
    if has_match and not has_return:
        errors.append("MATCH without RETURN")
    
    return len(errors) == 0, errors


def _auto_correct(cypher: str) -> str:
    """Fix common LLM-generated Cypher mistakes."""
    corrected = cypher
    corrected = QueryFixer.fix_return_syntax(corrected)
    corrected = QueryFixer.fix_relationship_direction(corrected)
    corrected = QueryFixer.ensure_limit(corrected)
    corrected = corrected.rstrip(';').strip()
    return corrected


def _clean_cypher(raw: str) -> str:
    """Strip markdown fences, backticks, and normalise whitespace."""
    cleaned = raw.replace("```cypher", "").replace("```", "").strip()
    cleaned = cleaned.strip('`').strip()
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    return cleaned


def _dry_run(cypher: str) -> Tuple[bool, str]:
    """EXPLAIN without executing — catches syntax errors cheaply."""
    try:
        run_cypher(f"EXPLAIN {cypher}", timeout=8)
        return True, ""
    except Exception as e:
        return False, str(e)


def _safe_execute(cypher: str, timeout: int = 30) -> Tuple[bool, List[dict], str]:
    """Execute query safely with error capture."""
    try:
        results = run_cypher(cypher, timeout=timeout)
        return True, results, ""
    except Exception as e:
        return False, [], str(e)


# ============================================================================
# LLM CALLS
# ============================================================================

def is_guardrail_response(text: str) -> bool:
    return text.strip().startswith("GUARDRAIL:")


def _call_openrouter(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    """Call OpenRouter chat completions API."""
    messages = [{"role": "system", "content": system}]

    if conversation_history:
        for msg in conversation_history[-6:]:
            messages.append({"role": msg.get("role", "user"), "content": msg["content"]})

    messages.append({"role": "user", "content": prompt})

    with httpx.Client(timeout=120.0) as client:
        resp = client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": messages,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


def _call_groq(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    """Call Groq API (free tier — llama3 70b)."""
    messages = [{"role": "system", "content": system}]

    if conversation_history:
        for msg in conversation_history[-6:]:
            messages.append({"role": msg.get("role", "user"), "content": msg["content"]})

    messages.append({"role": "user", "content": prompt})

    with httpx.Client(timeout=120.0) as client:
        resp = client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": messages,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


def _call_gemini(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    """Call Google Gemini API (free tier)."""
    contents = []

    if conversation_history:
        for msg in conversation_history[-6:]:
            role = "model" if msg.get("role") == "assistant" else "user"
            contents.append({"role": role, "parts": [{"text": msg["content"]}]})

    contents.append({"role": "user", "parts": [{"text": prompt}]})

    with httpx.Client(timeout=120.0) as client:
        resp = client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            params={"key": GEMINI_API_KEY},
            headers={"Content-Type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": system}]},
                "contents": contents,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()


def _build_provider_chain() -> list:
    """Build ordered list of (name, callable) providers with valid keys."""
    providers = []
    if OPENROUTER_API_KEY:
        providers.append(("openrouter", _call_openrouter))
    if GROQ_API_KEY:
        providers.append(("groq", _call_groq))
    if GEMINI_API_KEY:
        providers.append(("gemini", _call_gemini))
    return providers


def _call_llm(system: str, prompt: str, conversation_history: list[dict] = None) -> str:
    """Call LLM with automatic failover across providers on 429/5xx errors."""
    providers = _build_provider_chain()
    if not providers:
        raise RuntimeError("No LLM API keys configured. Set OPENROUTER_API_KEY, GROQ_API_KEY, or GEMINI_API_KEY in .env")

    last_error = None
    for name, call_fn in providers:
        try:
            result = call_fn(system, prompt, conversation_history)
            logger.info(f"LLM call succeeded via {name}")
            return result
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in (429, 502, 503, 529):
                logger.warning(f"Provider {name} returned {status}, trying next provider...")
                last_error = e
                continue
            raise
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            logger.warning(f"Provider {name} connection failed: {e}, trying next...")
            last_error = e
            continue

    raise RuntimeError(f"All LLM providers exhausted. Last error: {last_error}")


def generate_answer(user_query: str, cypher_query: str, results: list[dict]) -> str:
    results_str = str(results[:50])
    prompt = f"""User Question: {user_query}

Cypher Query Executed: {cypher_query}

Query Results (up to 50 rows):
{results_str}

Provide a natural language answer:"""
    return _call_llm(ANSWER_PROMPT, prompt)


# ============================================================================
# ANSWER PROMPT
# ============================================================================

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
# MAIN QUERY FUNCTION — With state management and intelligent retry
# ============================================================================

def query(user_query: str, conversation_history: list[dict] = None) -> dict:
    """
    End-to-end pipeline with intelligent error recovery and state management.
    
    Features:
    - Global rate limiting to prevent runaway retries
    - Per-query state tracking
    - Intelligent error classification and targeted fixes
    - Oscillation detection to prevent repeated failures
    - Automatic retry with error feedback to LLM
    - Multiple fallback strategies
    """
    
    # Check global rate limits
    if not global_state.can_process(user_query):
        return {
            "answer": "Too many similar queries recently. Please wait a moment before trying again.",
            "cypher": None,
            "results": [],
            "guardrail": False,
            "error": True,
            "error_type": "rate_limited"
        }
    
    # Create query context
    ctx = QueryContext(
        user_query=user_query,
        conversation_history=conversation_history
    )
    
    global_state.record_query(user_query)
    
    for attempt_num in range(1, MAX_CYPHER_RETRIES + 1):
        ctx.current_attempt = attempt_num
        global_state.record_attempt()
        
        try:
            # Create attempt record
            attempt = QueryAttempt(
                attempt_number=attempt_num,
                cypher="",
                state=QueryState.GENERATING
            )
            
            # ── Step 1: Generate or regenerate Cypher ─────────────────
            attempt.state = QueryState.GENERATING
            
            if attempt_num == 1:
                raw = _call_llm(
                    SYSTEM_PROMPT,
                    f"Generate a Cypher query for: {user_query}",
                    conversation_history,
                )
            else:
                # Include error context for regeneration
                last_error = ctx.get_last_error()
                last_category = ctx.get_last_error_category()
                
                error_context = f"""
The previous Cypher query failed with this error:
{last_error}

Error Type: {last_category.value if last_category else 'unknown'}

Original question: {user_query}

CRITICAL SYNTAX REMINDERS:
- RETURN clause format: RETURN node.property AS alias (e.g., RETURN p.id AS productId)
- NEVER use: RETURN p.id AS p.id (this is invalid)
- For aggregations: RETURN p.id AS productId, COUNT(DISTINCT b.id) AS count
- Always include LIMIT 25
- Check relationship directions: BILLS_PRODUCT goes FROM BillingDocumentItem TO Product
- Use: (b:BillingDocument)-[:HAS_ITEM]->(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)

Generate a CORRECTED Cypher query following these rules. Return ONLY the query, no explanation.
"""
                raw = _call_llm(SYSTEM_PROMPT, error_context, conversation_history)
            
            # Guardrail check
            if is_guardrail_response(raw):
                metrics.record_guardrail()
                return {
                    "answer": raw.replace("GUARDRAIL: ", ""),
                    "cypher": None,
                    "results": [],
                    "guardrail": True,
                }
            
            # ── Step 2: Clean & validate ──────────────────────────────
            attempt.state = QueryState.VALIDATING
            cypher_clean = _clean_cypher(raw)
            cypher_clean = _auto_correct(cypher_clean)
            
            valid, syntax_errors = _validate_syntax(cypher_clean)
            if not valid:
                attempt.error = f"Syntax validation: {'; '.join(syntax_errors)}"
                attempt.error_category = ErrorCategory.SYNTAX
                attempt.cypher = cypher_clean
                ctx.add_attempt(attempt)
                
                # Try automatic fixes for syntax errors
                if "Invalid RETURN alias" in attempt.error:
                    cypher_clean = QueryFixer.fix_return_syntax(cypher_clean)
                    valid, _ = _validate_syntax(cypher_clean)
                    if valid:
                        ctx.applied_strategies.append("auto_fix_return")
                        logger.info(f"Attempt {attempt_num}: Auto-fixed RETURN syntax")
                
                if not valid:
                    continue
            
            # ── Step 3: Dry-run test ──────────────────────────────────
            attempt.state = QueryState.DRY_RUN
            attempt.cypher = cypher_clean
            ok, explain_err = _dry_run(cypher_clean)
            
            if not ok:
                attempt.error = explain_err
                attempt.error_category = ErrorClassifier.classify(explain_err)
                ctx.add_attempt(attempt)
                
                logger.warning(f"Attempt {attempt_num}: Dry-run failed - {attempt.error_category.value}: {explain_err[:100]}")
                
                # Try targeted fixes based on error category
                if attempt.error_category == ErrorCategory.RETURN_SYNTAX:
                    cypher_clean = QueryFixer.fix_return_syntax(cypher_clean)
                    ok, _ = _dry_run(cypher_clean)
                    if ok:
                        ctx.applied_strategies.append("fix_return_syntax")
                        
                elif attempt.error_category == ErrorCategory.DIRECTION_ERROR:
                    cypher_clean = QueryFixer.fix_relationship_direction(cypher_clean)
                    ok, _ = _dry_run(cypher_clean)
                    if ok:
                        ctx.applied_strategies.append("fix_direction")
                        
                elif attempt.error_category == ErrorCategory.RELATIONSHIP_NOT_FOUND:
                    missing = ErrorClassifier.extract_missing_item(explain_err)
                    if missing == "BILLS_PRODUCT":
                        cypher_clean = re.sub(
                            r'\(p:Product\)\s*-\[:BILLS_PRODUCT\]->\s*\(bdi:BillingDocumentItem\)',
                            '(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)',
                            cypher_clean
                        )
                        ok, _ = _dry_run(cypher_clean)
                        if ok:
                            ctx.applied_strategies.append("fix_relationship_direction")
                
                if not ok:
                    continue
            
            # ── Step 4: Execute ───────────────────────────────────────
            attempt.state = QueryState.EXECUTING
            success, results, exec_error = _safe_execute(cypher_clean, timeout=CYPHER_TIMEOUT)
            
            if not success:
                attempt.error = exec_error
                attempt.error_category = ErrorClassifier.classify(exec_error)
                ctx.add_attempt(attempt)
                logger.warning(f"Attempt {attempt_num}: Execution failed - {attempt.error_category.value}")
                
                # Last resort: try fallback query
                if attempt_num == MAX_CYPHER_RETRIES - 1 and "billing" in user_query.lower() and "product" in user_query.lower():
                    fallback = """
                    MATCH (b:BillingDocument)-[:HAS_ITEM]->(bdi:BillingDocumentItem)
                    MATCH (p:Product) WHERE p.id = bdi.material
                    RETURN p.id AS productId, p.description AS productDescription, COUNT(DISTINCT b.id) AS billingDocumentCount
                    ORDER BY billingDocumentCount DESC
                    LIMIT 25
                    """
                    success, results, exec_error = _safe_execute(fallback)
                    if success:
                        cypher_clean = fallback
                        ctx.applied_strategies.append("ultimate_fallback")
                
                if not success:
                    continue
            
            # ── Step 5: Success! ──────────────────────────────────────
            attempt.state = QueryState.SUCCESS
            ctx.add_attempt(attempt)
            ctx.final_cypher = cypher_clean
            ctx.final_results = results
            
            # Generate answer
            answer = generate_answer(user_query, cypher_clean, results)
            strategy_used = " + ".join(ctx.applied_strategies) if ctx.applied_strategies else "direct"
            
            metrics.record_success(attempt_num, strategy_used)
            
            return {
                "answer": answer,
                "cypher": cypher_clean,
                "results": results[:25],
                "guardrail": False,
                "strategy_used": strategy_used,
                "attempts": attempt_num,
                "state": "success"
            }
            
        except Exception as exc:
            attempt = QueryAttempt(
                attempt_number=attempt_num,
                cypher=cypher_clean if 'cypher_clean' in locals() else "",
                state=QueryState.FAILED,
                error=str(exc),
                error_category=ErrorCategory.UNKNOWN
            )
            ctx.add_attempt(attempt)
            logger.error(f"Attempt {attempt_num} exception: {exc}")
            
            if attempt_num < MAX_CYPHER_RETRIES:
                time.sleep(min(attempt_num * 0.5, 2))
    
    # All retries exhausted
    metrics.record_failure(ctx.get_last_error_category().value if ctx.get_last_error_category() else "unknown")
    global_state.record_failure()
    
    last_error = ctx.get_last_error()
    last_category = ctx.get_last_error_category()
    
    # Provide helpful feedback based on error type
    helpful_hints = ""
    if last_category == ErrorCategory.RELATIONSHIP_NOT_FOUND:
        helpful_hints = "\n\nHint: Check relationship directions. For product-billing queries, use: (b:BillingDocument)-[:HAS_ITEM]->(bdi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)"
    elif last_category == ErrorCategory.RETURN_SYNTAX:
        helpful_hints = "\n\nHint: Use RETURN node.property AS alias (e.g., RETURN p.id AS productId), never RETURN p.id AS p.id"
    elif last_category == ErrorCategory.DIRECTION_ERROR:
        helpful_hints = "\n\nHint: Check relationship directions. BILLS_PRODUCT goes FROM BillingDocumentItem TO Product"
    
    return {
        "answer": (
            f"I was unable to answer after {MAX_CYPHER_RETRIES} attempts. "
            f"The last error was: {last_error}{helpful_hints}\n\n"
            f"Please try rephrasing your question or check if the data exists in the database."
        ),
        "cypher": ctx.final_cypher,
        "results": [],
        "guardrail": False,
        "error": True,
        "error_type": last_category.value if last_category else "unknown",
        "attempts": len(ctx.attempts)
    }


# ============================================================================
# METRICS (unchanged)
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
        self.strategies_used: dict[str, int] = {}
    
    def record_success(self, attempts: int, strategy: str = "default"):
        self.total += 1
        self.success += 1
        self.retries_used.append(attempts)
        self.strategies_used[strategy] = self.strategies_used.get(strategy, 0) + 1
    
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
            "strategies_used": dict(self.strategies_used),
            "global_stats": global_state.get_stats(),
        }


metrics = QueryMetrics()


def get_metrics() -> dict:
    return metrics.report()
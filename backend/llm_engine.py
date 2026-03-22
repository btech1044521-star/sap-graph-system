"""
LLM-powered query engine: Translates natural language to Cypher queries
using Google Gemini, executes them against Neo4j, and returns natural language answers.
"""

import google.generativeai as genai
from config import GEMINI_API_KEY
from database import run_cypher

genai.configure(api_key=GEMINI_API_KEY)

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


def is_guardrail_response(text: str) -> bool:
    return text.strip().startswith("GUARDRAIL:")


def generate_cypher(user_query: str, conversation_history: list[dict] = None) -> str:
    """Use Gemini to translate natural language to Cypher."""
    model = genai.GenerativeModel("gemini-2.0-flash")

    messages = [{"role": "user", "parts": [SYSTEM_PROMPT]}]
    messages.append({"role": "model", "parts": ["Understood. I will generate Cypher queries for SAP O2C data or return a GUARDRAIL message for off-topic questions."]})

    if conversation_history:
        for msg in conversation_history[-6:]:
            messages.append({"role": msg["role"], "parts": [msg["content"]]})

    messages.append({"role": "user", "parts": [f"Generate a Cypher query for: {user_query}"]})

    response = model.generate_content(messages)
    return response.text.strip()


def generate_answer(user_query: str, cypher_query: str, results: list[dict]) -> str:
    """Use Gemini to generate a natural language answer from query results."""
    model = genai.GenerativeModel("gemini-2.0-flash")

    results_str = str(results[:50])  # Cap to prevent token overflow

    prompt = f"""{ANSWER_PROMPT}

User Question: {user_query}

Cypher Query Executed: {cypher_query}

Query Results (up to 50 rows):
{results_str}

Provide a natural language answer:"""

    response = model.generate_content(prompt)
    return response.text.strip()


def query(user_query: str, conversation_history: list[dict] = None) -> dict:
    """
    Main query function:
    1. Generate Cypher from natural language
    2. Execute against Neo4j
    3. Generate natural language answer
    """
    try:
        cypher = generate_cypher(user_query, conversation_history)

        if is_guardrail_response(cypher):
            return {
                "answer": cypher.replace("GUARDRAIL: ", ""),
                "cypher": None,
                "results": [],
                "guardrail": True
            }

        # Clean up any markdown formatting the LLM might add
        cypher_clean = cypher.replace("```cypher", "").replace("```", "").strip()

        results = run_cypher(cypher_clean)

        answer = generate_answer(user_query, cypher_clean, results)

        return {
            "answer": answer,
            "cypher": cypher_clean,
            "results": results[:25],
            "guardrail": False
        }

    except Exception as e:
        return {
            "answer": f"I encountered an error processing your query. Please try rephrasing. Error: {str(e)}",
            "cypher": cypher_clean if 'cypher_clean' in dir() else None,
            "results": [],
            "guardrail": False,
            "error": True
        }

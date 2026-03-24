"""
Data ingestion script: Loads SAP O2C JSONL data into Neo4j graph.

Graph Model:
  Nodes: 
    - Customer, SalesOrder, SalesOrderItem, Delivery, DeliveryItem
    - BillingDocument, BillingDocumentItem, JournalEntry, Payment
    - Product, Plant, Address

  Relationships (with correct directions):
    - (Customer)-[:PLACED_ORDER]->(SalesOrder)
    - (SalesOrder)-[:HAS_ITEM]->(SalesOrderItem)
    - (SalesOrderItem)-[:CONTAINS_PRODUCT]->(Product)
    - (SalesOrderItem)-[:FULFILLED_FROM_PLANT]->(Plant)
    
    - (Delivery)-[:HAS_ITEM]->(DeliveryItem)
    - (SalesOrder)-[:FULFILLED_BY]->(DeliveryItem)
    - (DeliveryItem)-[:SHIPPED_FROM]->(Plant)
    
    - (BillingDocument)-[:HAS_ITEM]->(BillingDocumentItem)
    - (BillingDocumentItem)-[:BILLS_PRODUCT]->(Product)  # Note: direction is BillingDocumentItem → Product
    - (SalesOrder)-[:BILLED_IN]->(BillingDocumentItem)
    - (Customer)-[:BILLED_TO]->(BillingDocument)
    
    - (BillingDocument)-[:GENERATES_JOURNAL_ENTRY]->(JournalEntry)
    - (JournalEntry)-[:POSTED_FOR]->(Customer)
    
    - (Payment)-[:PAID_BY]->(Customer)
    - (Payment)-[:PAYS_FOR]->(SalesOrder)
    
    - (Customer)-[:HAS_ADDRESS]->(Address)
    - (Product)-[:PRODUCED_AT]->(Plant)

Key Path for Billing Analysis:
  (BillingDocument)-[:HAS_ITEM]->(BillingDocumentItem)-[:BILLS_PRODUCT]->(Product)
  This path answers: Which products appear in which billing documents?
"""

import json
import os
import glob
import time
from database import Neo4jConnection, get_session
from config import DATA_DIR


def read_jsonl_files(entity_dir: str) -> list[dict]:
    """Read all JSONL files in a directory and return records."""
    records = []
    pattern = os.path.join(DATA_DIR, entity_dir, "*.jsonl")
    for filepath in glob.glob(pattern):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    print(f"  Read {len(records)} records from {entity_dir}")
    return records


def create_constraints(session):
    """Create uniqueness constraints for fast lookups."""
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SalesOrder) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (si:SalesOrderItem) REQUIRE si.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Delivery) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (di:DeliveryItem) REQUIRE di.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (b:BillingDocument) REQUIRE b.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (bi:BillingDocumentItem) REQUIRE bi.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (j:JournalEntry) REQUIRE j.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pl:Plant) REQUIRE pl.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE",
    ]
    for c in constraints:
        session.run(c)
    print("Constraints created.")


def ingest_customers(session):
    """Load business partners as Customer nodes."""
    print("Loading Customers...")
    records = read_jsonl_files("business_partners")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (c:Customer {id: r.businessPartner})
            SET c.name = r.businessPartnerFullName,
                c.shortName = r.businessPartnerName,
                c.category = r.businessPartnerCategory,
                c.grouping = r.businessPartnerGrouping,
                c.language = r.correspondenceLanguage,
                c.firstName = r.firstName,
                c.lastName = r.lastName,
                c.orgName = r.organizationBpName1,
                c.industry = r.industry,
                c.isBlocked = r.businessPartnerIsBlocked,
                c.creationDate = r.creationDate
        """, {"batch": batch})


def ingest_addresses(session):
    """Load addresses and link to customers."""
    print("Loading Addresses...")
    records = read_jsonl_files("business_partner_addresses")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (a:Address {id: r.businessPartner + '_' + r.addressId})
            SET a.city = r.cityName,
                a.country = r.country,
                a.postalCode = r.postalCode,
                a.region = r.region,
                a.street = r.streetName,
                a.addressId = r.addressId
            WITH a, r
            MATCH (c:Customer {id: r.businessPartner})
            MERGE (c)-[:HAS_ADDRESS]->(a)
        """, {"batch": batch})


def ingest_products(session):
    """Load products."""
    print("Loading Products...")
    records = read_jsonl_files("products")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (p:Product {id: r.product})
            SET p.type = r.productType,
                p.group = r.productGroup,
                p.baseUnit = r.baseUnit,
                p.grossWeight = r.grossWeight,
                p.netWeight = r.netWeight,
                p.weightUnit = r.weightUnit,
                p.division = r.division,
                p.creationDate = r.creationDate
        """, {"batch": batch})

    # Add descriptions
    desc_records = read_jsonl_files("product_descriptions")
    for batch_start in range(0, len(desc_records), 500):
        batch = desc_records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MATCH (p:Product {id: r.product})
            SET p.description = r.productDescription
        """, {"batch": batch})


def ingest_plants(session):
    """Load plants."""
    print("Loading Plants...")
    records = read_jsonl_files("plants")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (pl:Plant {id: r.plant})
            SET pl.name = r.plantName,
                pl.salesOrganization = r.salesOrganization,
                pl.distributionChannel = r.distributionChannel,
                pl.language = r.language
        """, {"batch": batch})

    # Product-Plant assignments (PRODUCED_AT relationship)
    pp_records = read_jsonl_files("product_plants")
    for batch_start in range(0, len(pp_records), 500):
        batch = pp_records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MATCH (p:Product {id: r.product})
            MATCH (pl:Plant {id: r.plant})
            MERGE (p)-[:PRODUCED_AT]->(pl)
        """, {"batch": batch})


def ingest_sales_orders(session):
    """Load sales orders, items, and schedule lines."""
    print("Loading Sales Orders...")
    records = read_jsonl_files("sales_order_headers")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (so:SalesOrder {id: r.salesOrder})
            SET so.type = r.salesOrderType,
                so.salesOrg = r.salesOrganization,
                so.distributionChannel = r.distributionChannel,
                so.totalNetAmount = toFloat(r.totalNetAmount),
                so.currency = r.transactionCurrency,
                so.creationDate = r.creationDate,
                so.deliveryStatus = r.overallDeliveryStatus,
                so.billingStatus = r.overallOrdReltdBillgStatus,
                so.requestedDeliveryDate = r.requestedDeliveryDate,
                so.paymentTerms = r.customerPaymentTerms,
                so.soldToParty = r.soldToParty
            WITH so, r
            MATCH (c:Customer {id: r.soldToParty})
            MERGE (c)-[:PLACED_ORDER]->(so)
        """, {"batch": batch})

    # Sales Order Items
    print("Loading Sales Order Items...")
    items = read_jsonl_files("sales_order_items")
    for batch_start in range(0, len(items), 500):
        batch = items[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (si:SalesOrderItem {id: r.salesOrder + '_' + r.salesOrderItem})
            SET si.salesOrder = r.salesOrder,
                si.itemNumber = r.salesOrderItem,
                si.category = r.salesOrderItemCategory,
                si.material = r.material,
                si.requestedQuantity = toFloat(r.requestedQuantity),
                si.quantityUnit = r.requestedQuantityUnit,
                si.netAmount = toFloat(r.netAmount),
                si.currency = r.transactionCurrency,
                si.plant = r.productionPlant,
                si.storageLocation = r.storageLocation
            WITH si, r
            MATCH (so:SalesOrder {id: r.salesOrder})
            MERGE (so)-[:HAS_ITEM]->(si)
            WITH si, r
            MATCH (p:Product {id: r.material})
            MERGE (si)-[:CONTAINS_PRODUCT]->(p)
            WITH si, r
            MATCH (pl:Plant {id: r.productionPlant})
            MERGE (si)-[:FULFILLED_FROM_PLANT]->(pl)
        """, {"batch": batch})

    # Schedule lines
    print("Loading Schedule Lines...")
    slines = read_jsonl_files("sales_order_schedule_lines")
    for batch_start in range(0, len(slines), 500):
        batch = slines[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MATCH (si:SalesOrderItem {id: r.salesOrder + '_' + r.salesOrderItem})
            SET si.confirmedDeliveryDate = r.confirmedDeliveryDate,
                si.confirmedQuantity = toFloat(r.confdOrderQtyByMatlAvailCheck)
        """, {"batch": batch})


def ingest_deliveries(session):
    """Load outbound deliveries."""
    print("Loading Deliveries...")
    records = read_jsonl_files("outbound_delivery_headers")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (d:Delivery {id: r.deliveryDocument})
            SET d.goodsMovementDate = r.actualGoodsMovementDate,
                d.creationDate = r.creationDate,
                d.shippingPoint = r.shippingPoint,
                d.pickingStatus = r.overallPickingStatus,
                d.goodsMovementStatus = r.overallGoodsMovementStatus,
                d.incompletionStatus = r.hdrGeneralIncompletionStatus
        """, {"batch": batch})

    # Delivery Items
    print("Loading Delivery Items...")
    items = read_jsonl_files("outbound_delivery_items")
    for batch_start in range(0, len(items), 500):
        batch = items[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (di:DeliveryItem {id: r.deliveryDocument + '_' + r.deliveryDocumentItem})
            SET di.deliveryDocument = r.deliveryDocument,
                di.itemNumber = r.deliveryDocumentItem,
                di.quantity = toFloat(r.actualDeliveryQuantity),
                di.quantityUnit = r.deliveryQuantityUnit,
                di.plant = r.plant,
                di.storageLocation = r.storageLocation,
                di.referenceSdDocument = r.referenceSdDocument,
                di.referenceSdDocumentItem = r.referenceSdDocumentItem
            WITH di, r
            MATCH (d:Delivery {id: r.deliveryDocument})
            MERGE (d)-[:HAS_ITEM]->(di)
            WITH di, r
            MATCH (so:SalesOrder {id: r.referenceSdDocument})
            MERGE (so)-[:FULFILLED_BY]->(di)
            WITH di, r
            MATCH (pl:Plant {id: r.plant})
            MERGE (di)-[:SHIPPED_FROM]->(pl)
        """, {"batch": batch})


def ingest_billing_documents(session):
    """Load billing documents and items."""
    print("Loading Billing Documents...")
    records = read_jsonl_files("billing_document_headers")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (b:BillingDocument {id: r.billingDocument})
            SET b.type = r.billingDocumentType,
                b.creationDate = r.creationDate,
                b.billingDate = r.billingDocumentDate,
                b.isCancelled = r.billingDocumentIsCancelled,
                b.cancelledDocument = r.cancelledBillingDocument,
                b.totalNetAmount = toFloat(r.totalNetAmount),
                b.currency = r.transactionCurrency,
                b.companyCode = r.companyCode,
                b.fiscalYear = r.fiscalYear,
                b.accountingDocument = r.accountingDocument,
                b.soldToParty = r.soldToParty
            WITH b, r
            MATCH (c:Customer {id: r.soldToParty})
            MERGE (c)-[:BILLED_TO]->(b)
        """, {"batch": batch})

    # Cancelled billing docs
    print("Loading Billing Cancellations...")
    cancellations = read_jsonl_files("billing_document_cancellations")
    for batch_start in range(0, len(cancellations), 500):
        batch = cancellations[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (b:BillingDocument {id: r.billingDocument})
            SET b.type = r.billingDocumentType,
                b.isCancelled = r.billingDocumentIsCancelled,
                b.cancelledDocument = r.cancelledBillingDocument,
                b.totalNetAmount = toFloat(r.totalNetAmount),
                b.currency = r.transactionCurrency,
                b.companyCode = r.companyCode
        """, {"batch": batch})

    # Billing Items - Creates HAS_ITEM, BILLED_IN, and BILLS_PRODUCT relationships
    print("Loading Billing Document Items...")
    items = read_jsonl_files("billing_document_items")
    for batch_start in range(0, len(items), 500):
        batch = items[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (bi:BillingDocumentItem {id: r.billingDocument + '_' + r.billingDocumentItem})
            SET bi.billingDocument = r.billingDocument,
                bi.itemNumber = r.billingDocumentItem,
                bi.material = r.material,
                bi.quantity = toFloat(r.billingQuantity),
                bi.quantityUnit = r.billingQuantityUnit,
                bi.netAmount = toFloat(r.netAmount),
                bi.currency = r.transactionCurrency,
                bi.referenceSdDocument = r.referenceSdDocument,
                bi.referenceSdDocumentItem = r.referenceSdDocumentItem
            
            // Connect to BillingDocument
            WITH bi, r
            MATCH (b:BillingDocument {id: r.billingDocument})
            MERGE (b)-[:HAS_ITEM]->(bi)
            
            // Connect to SalesOrder (if exists)
            WITH bi, r
            MATCH (so:SalesOrder {id: r.referenceSdDocument})
            MERGE (so)-[:BILLED_IN]->(bi)
            
            // Connect to Product - Direction: BillingDocumentItem → Product
            WITH bi, r
            MATCH (p:Product {id: r.material})
            MERGE (bi)-[:BILLS_PRODUCT]->(p)
        """, {"batch": batch})


def ingest_journal_entries(session):
    """Load journal entries (accounts receivable)."""
    print("Loading Journal Entries...")
    records = read_jsonl_files("journal_entry_items_accounts_receivable")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (j:JournalEntry {id: r.companyCode + '_' + r.fiscalYear + '_' + r.accountingDocument + '_' + r.accountingDocumentItem})
            SET j.companyCode = r.companyCode,
                j.fiscalYear = r.fiscalYear,
                j.accountingDocument = r.accountingDocument,
                j.documentType = r.accountingDocumentType,
                j.itemNumber = r.accountingDocumentItem,
                j.glAccount = r.glAccount,
                j.amount = toFloat(r.amountInTransactionCurrency),
                j.currency = r.transactionCurrency,
                j.localAmount = toFloat(r.amountInCompanyCodeCurrency),
                j.localCurrency = r.companyCodeCurrency,
                j.postingDate = r.postingDate,
                j.documentDate = r.documentDate,
                j.customer = r.customer,
                j.profitCenter = r.profitCenter,
                j.clearingDate = r.clearingDate,
                j.clearingDocument = r.clearingAccountingDocument,
                j.referenceDocument = r.referenceDocument
            WITH j, r
            MATCH (b:BillingDocument {accountingDocument: r.accountingDocument, companyCode: r.companyCode})
            MERGE (b)-[:GENERATES_JOURNAL_ENTRY]->(j)
            WITH j, r
            MATCH (c:Customer {id: r.customer})
            MERGE (j)-[:POSTED_FOR]->(c)
        """, {"batch": batch})


def ingest_payments(session):
    """Load payments (accounts receivable)."""
    print("Loading Payments...")
    records = read_jsonl_files("payments_accounts_receivable")
    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        session.run("""
            UNWIND $batch AS r
            MERGE (pay:Payment {id: r.companyCode + '_' + r.fiscalYear + '_' + r.accountingDocument + '_' + r.accountingDocumentItem})
            SET pay.companyCode = r.companyCode,
                pay.fiscalYear = r.fiscalYear,
                pay.accountingDocument = r.accountingDocument,
                pay.itemNumber = r.accountingDocumentItem,
                pay.amount = toFloat(r.amountInTransactionCurrency),
                pay.currency = r.transactionCurrency,
                pay.localAmount = toFloat(r.amountInCompanyCodeCurrency),
                pay.localCurrency = r.companyCodeCurrency,
                pay.postingDate = r.postingDate,
                pay.documentDate = r.documentDate,
                pay.customer = r.customer,
                pay.glAccount = r.glAccount,
                pay.profitCenter = r.profitCenter,
                pay.clearingDate = r.clearingDate,
                pay.clearingDocument = r.clearingAccountingDocument,
                pay.invoiceReference = r.invoiceReference,
                pay.salesDocument = r.salesDocument
            WITH pay, r
            MATCH (c:Customer {id: r.customer})
            MERGE (pay)-[:PAID_BY]->(c)
            WITH pay, r
            WHERE r.salesDocument IS NOT NULL AND r.salesDocument <> ''
            MATCH (so:SalesOrder {id: r.salesDocument})
            MERGE (pay)-[:PAYS_FOR]->(so)
        """, {"batch": batch})


def create_indexes(session):
    """Create additional indexes for query performance."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (b:BillingDocument) ON (b.accountingDocument)",
        "CREATE INDEX IF NOT EXISTS FOR (b:BillingDocument) ON (b.soldToParty)",
        "CREATE INDEX IF NOT EXISTS FOR (j:JournalEntry) ON (j.accountingDocument)",
        "CREATE INDEX IF NOT EXISTS FOR (j:JournalEntry) ON (j.customer)",
        "CREATE INDEX IF NOT EXISTS FOR (so:SalesOrder) ON (so.soldToParty)",
        "CREATE INDEX IF NOT EXISTS FOR (pay:Payment) ON (pay.customer)",
        "CREATE INDEX IF NOT EXISTS FOR (di:DeliveryItem) ON (di.referenceSdDocument)",
        "CREATE INDEX IF NOT EXISTS FOR (bi:BillingDocumentItem) ON (bi.referenceSdDocument)",
        "CREATE INDEX IF NOT EXISTS FOR (bi:BillingDocumentItem) ON (bi.material)",
        "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.id)",
    ]
    for idx in indexes:
        session.run(idx)
    print("Indexes created.")


def main():
    print("=" * 60)
    print("SAP O2C Data Ingestion into Neo4j")
    print("=" * 60)

    driver = Neo4jConnection.get_driver()

    with driver.session() as session:
        start = time.time()

        print("\n[1/11] Creating constraints...")
        create_constraints(session)

        print("\n[2/11] Ingesting Customers...")
        ingest_customers(session)

        print("\n[3/11] Ingesting Addresses...")
        ingest_addresses(session)

        print("\n[4/11] Ingesting Products...")
        ingest_products(session)

        print("\n[5/11] Ingesting Plants...")
        ingest_plants(session)

        print("\n[6/11] Ingesting Sales Orders...")
        ingest_sales_orders(session)

        print("\n[7/11] Ingesting Deliveries...")
        ingest_deliveries(session)

        print("\n[8/11] Ingesting Billing Documents...")
        ingest_billing_documents(session)

        print("\n[9/11] Ingesting Journal Entries...")
        ingest_journal_entries(session)

        print("\n[10/11] Ingesting Payments...")
        ingest_payments(session)

        print("\n[11/11] Creating additional indexes...")
        create_indexes(session)

        elapsed = time.time() - start
        print(f"\n✅ Ingestion complete in {elapsed:.1f}s")

        # Print stats
        result = session.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY count DESC")
        print("\n📊 Node counts:")
        for r in result:
            print(f"  {r['label']}: {r['count']}")

        result = session.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC")
        print("\n🔗 Relationship counts:")
        for r in result:
            print(f"  {r['type']}: {r['count']}")

    Neo4jConnection.close()
    print("\n🎉 Migration to AuraDB complete!")


if __name__ == "__main__":
    main()
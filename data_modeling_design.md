# Data Modeling: SQL and NoSQL Design Decisions

This document explains the **data modeling approach** for the four core datasets used in this Databricks project:

- `customers`
- `products`
- `orders`
- `events`

Each dataset is evaluated for **Relational (SQL)** and **NoSQL** modeling, with clear justification of **why SQL or NoSQL is the better fit**.

---

## 1. Overview of Datasets

| Dataset | Nature | Cardinality | Usage Pattern |
|-------|------|------------|---------------|
| Customers | Master data | Low–Medium | Lookups, joins |
| Products | Master data | Low–Medium | Lookups, joins |
| Orders | Transactional | High | Analytics, aggregations |
| Events | Semi-structured | Very High | Clickstream, behavioral analytics |

---

## 2. Relational Database Modeling (SQL)

Relational modeling is used for **customers, products, and orders** because these datasets benefit from **structured schema, constraints, and joins**.

### 2.1 Customer Dimension (`dim_customer`)

**Design Type:** Normalized Dimension Table

```sql
customer_id (PK)
customer_name
email
country
city
registration_date
customer_segment
```

**Keys**
- **Primary Key:** `customer_id`

**Why SQL?**
- Strong entity definition
- Frequently joined with fact tables
- Benefits from constraints and indexing

---

### 2.2 Product Dimension (`dim_product`)

**Design Type:** Normalized Dimension Table

```sql
product_id (PK)
product_name
category
brand
cost_price
list_price
```

**Keys**
- **Primary Key:** `product_id`

**Why SQL?**
- Structured attributes
- Used in aggregations and joins
- Pricing requires numeric precision

---

### 2.3 Order Fact (`fact_order`)

**Design Type:** Analytics-friendly Fact Table (Star Schema)

```sql
order_id (PK)
customer_id (FK)
product_id (FK)
order_date
quantity
unit_price
status
payment_method
```

**Keys**
- **Primary Key:** `order_id`
- **Foreign Keys:**
  - `customer_id` → dim_customer
  - `product_id` → dim_product

**Why SQL?**
- Supports aggregations (revenue, quantity)
- Referential integrity matters
- Optimized for BI tools and reporting

---

### 2.4 SQL Modeling Summary

| Dataset | SQL Model | Reason |
|------|----------|-------|
| Customers | Dimension | Stable master data |
| Products | Dimension | Structured attributes |
| Orders | Fact | Transactional analytics |

---

## 3. NoSQL Database Modeling

NoSQL is evaluated primarily for the **events dataset**, which is semi-structured and high-volume.

---

### 3.1 Event Data (`events`)

**Recommended NoSQL Model:** Document Store (e.g., MongoDB, Cosmos DB)

**Sample Document**
```json
{
  "event_id": "E001",
  "timestamp": "2024-01-15T10:23:45Z",
  "event_type": "page_view",
  "user_id": "C001",
  "session_id": "S1001",
  "attributes": {
    "page_url": "/products/wireless-mouse",
    "browser": "Chrome",
    "device_type": "desktop",
    "duration_seconds": 45
  }
}
```

**Why Document Model?**
- Events vary by type (page_view, checkout, search)
- Flexible schema avoids frequent ALTER operations
- Nested attributes are natural for JSON

---

### 3.2 Alternative NoSQL Models (Rejected)

| Model | Reason Rejected |
|-----|----------------|
| Key-Value | Too simple, no querying |
| Column Store | Less flexible for nested event payloads |
| Graph | Overkill for event processing |

---

## 4. Why This Hybrid Approach Works

- **SQL** ensures:
  - Data integrity
  - Reliable analytics
  - BI tool compatibility

- **NoSQL** ensures:
  - Schema flexibility
  - High write throughput
  - Easy evolution of event payloads

In this project:
- SQL (Delta Lake) is used for **analytics-ready Silver tables**
- NoSQL concepts guide how **event data is ingested and flattened**

---

## 5. Final Recommendation

| Layer | Technology | Reason |
|-----|-----------|-------|
| Dimensions | SQL / Delta Lake | Consistency & joins |
| Facts | SQL / Delta Lake | Aggregations & reporting |
| Events | NoSQL (Document-style) | Flexibility & scale |

---

## 7. Conclusion

The data model follows **best-practice enterprise patterns**:
- Star-schema for analytics
- Document model for behavioral data
- Hybrid SQL + NoSQL strategy for scalability


1. Approach

The objective was to design and implement a simple but production-shaped data pipeline using:

Python 3.10+
PostgreSQL 14+
pandas
psycopg v3

The pipeline processes structured transactional data (customers, orders, order_items), applies validation and transformation logic, enforces referential integrity, and exposes analytical views.

The solution follows a layered architecture:

Landing layer → raw validated tables

structure layer → analytical views and data quality checks

2. Architecture
High-Level Flow
Files (CSV / JSONL)
        ↓
Ingestion (pandas)
        ↓
Transformation & Validation
        ↓
Load to PostgreSQL (FK enforced)
        ↓
Curated & DQ SQL Views

The pipeline is executed via:
    python -m src.main
3. Database Design
Schemas
landing → raw transactional tables
structured → analytical views

Tables
landing.customers
Primary key: customer_id
Unique constraint: email
Email enforced lowercase
Signup date stored as DATE

landing.orders
Primary key: order_id
Foreign key → customers(customer_id)
Status constrained via CHECK
order_ts stored as TIMESTAMPTZ

landing.order_items
Composite primary key: (order_id, line_no)
Foreign key → orders(order_id)
Quantity and price constrained to positive values
All referential integrity is enforced at the database level.

4. Ingestion Layer
Reads from CSV or JSONL
Config-driven using config.yaml
Returns dictionary of {dataset_name: DataFrame}
No transformations happen during ingestion
This keeps ingestion purely responsible for reading data.

5. Transformation & Validation
Transformation enforces deterministic data cleaning:
Standardization
Emails converted to lowercase
Text fields trimmed
Status normalized via mapping
Timestamps converted to UTC
Date-only fields preserved as DATE
Data Quality Handling
Invalid email format → rejected
Duplicate customers by email → earliest signup retained
Orders referencing missing customers → rejected
Missing primary keys → rejected
Non-positive quantities → corrected via absolute value
Invalid statuses → mapped or rejected
Rejected records are captured separately.

The philosophy:
Clean what is safe to clean, reject what breaks integrity.

6. Loading Strategy
Data is loaded respecting foreign key order:
customers
orders
order_items

Primary key conflicts are handled using staging + ON CONFLICT logic.

Referential integrity is not disabled; instead, upstream validation ensures only valid rows reach the database.

7. Analytical Views
The curated layer includes:

1. Daily Metrics
date
orders_count
total_revenue
average_order_value
Revenue calculated from order_items to ensure accuracy.

2. Top 10 Customers by Lifetime Spend

Aggregates total spend per customer across valid orders.

3. Top 10 SKUs by Revenue and Units Sold

Aggregates SKU-level performance.

8. Data Quality Views
Duplicate Customers by Lowercase Email

Identifies email collisions even if casing differs.

Orders Referencing Missing Customers

Identifies referential integrity violations if they occur upstream.

9. Logging
Basic structured logging implemented:
Stage start/end
Execution duration per stage
Pipeline total duration
Error logging with stack trace

Example:

[INFO] Ingest started
[INFO] Ingest completed in 0.42s
10. Design Decisions
Why Enforce FK Constraints?

Data integrity should be enforced at the database layer, not just in code.

Why Reject Instead of Silently Fix?

Incorrect foreign keys and missing primary keys indicate corrupted data and should not be silently corrected.

Why Separate Landing and Curated?

Separating raw ingestion from analytical views improves maintainability and aligns with medallion architecture principles.

Why Use Views Instead of Materialized Tables?

Views ensure:

No data duplication

Always up-to-date results

Clear business logic encapsulation

11. Assumptions

Cancelled and refunded orders excluded from revenue metrics

Emails must be valid and lowercase

Quantity must be positive

Orders must reference existing customers

Duplicate customers resolved by earliest signup date

12. Limitations

Important parts are hard coded , change in files or structure could break pipeline
No orchestration framework
Logging limited to stdout
No automated testing suite included

13. Possible Improvements
Add unit tests for transformation functions
Add metrics (rows processed, rows rejected)

Conclusion
The solution prioritizes:
Data integrity
Deterministic transformations
Clear separation of concerns
Production-aligned database modeling
Clean analytical abstraction


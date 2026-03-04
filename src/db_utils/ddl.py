from psycopg import sql
from src.db_utils.db_connect import get_conn

DDL = [
    # schema separation
    "create schema if not exists landing;",
    "create schema if not exists structured;",

    # ingestion landing table (raw-ish)
    """
    create table if not exists landing.customers (
        customer_id INTEGER NOT NULL
        ,email TEXT NOT NULL UNIQUE CHECK(email = lower(email)) NOT NULL
        ,full_name TEXT
        ,signup_date DATE NOT NULL
        ,country_code CHAR(2)
        ,is_active BOOLEAN
        ,primary key(customer_id)
    );
    """
    ,
    
    """
    create table if not exists landing.orders(
        order_id BIGINT NOT NULL
        ,customer_id INTEGER NOT NULL
        ,order_ts TIMESTAMPTZ NOT NULL
        ,status TEXT NOT NULL CHECK (status IN ('placed','shipped','cancelled','refunded'))
        ,total_amount NUMERIC(12,2)
        ,currency CHAR(3)
        ,primary key (order_id)
        ,foreign key(customer_id) references landing.customers(customer_id)
    );
    """,
    
    """
    create table if not exists landing.order_items (
        order_id BIGINT NOT NULL
        ,line_no INTEGER NOT NULL 
        ,sku TEXT NOT NULL
        ,quantity INTEGER NOT NULL CHECK(quantity > 0)
        ,unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price >= 0)
        ,category TEXT
        ,primary key (order_id, line_no)
        ,foreign key(order_id) references landing.orders(order_id)
        
    );
    """,

]

def run_ddl():
    with get_conn() as conn:
        with conn.cursor() as cur:
            
            for stmt in DDL:
                cur.execute(stmt)
                print(stmt.strip().splitlines()[0], "OK")
if __name__ == "__main__":
    run_ddl()
    
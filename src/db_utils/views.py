from src.db_utils.db_connect import get_conn




sql = [ 
    """

    create or replace view structured.sku_revenue as
    select
        oi.sku,
        sum(oi.quantity)                 as total_units_sold,
        sum(oi.quantity * oi.unit_price) as total_revenue
    from landing.order_items oi
    join landing.orders o
        on oi.order_id = o.order_id
    where o.status != 'cancelled'
    group by oi.sku;
    """,
    """
    create or replace view structured.top_10_customers_lifetime_spend as
    with order_revenue as (
        select
            o.order_id,
            o.customer_id,
            sum(oi.quantity * oi.unit_price) as order_revenue
        from landing.orders o
        join landing.order_items oi
        on oi.order_id = o.order_id
        where o.status in ('placed', 'shipped')
        group by o.order_id, o.customer_id
        )
    select
        c.customer_id,
        c.email,
        c.full_name,
        sum(orv.order_revenue)::numeric(14,2) as lifetime_spend,
        count(distinct orv.order_id)::int     as orders_count
    from order_revenue orv
    join landing.customers c
    on c.customer_id = orv.customer_id
    group by c.customer_id, c.email, c.full_name
    order by lifetime_spend desc
    limit 10;
    """
    ,

    """
    create or replace view structured.top_10_skus as
    select
        oi.sku,
        sum(oi.quantity)::int as units_sold,
        sum(oi.quantity * oi.unit_price)::numeric(14,2) as revenue
    from landing.order_items oi
    join landing.orders o
    on o.order_id = oi.order_id
    where o.status in ('placed', 'shipped')
    group by oi.sku
    order by revenue desc, units_sold desc
    limit 10;
    """
    ,
    """
    create or replace view structured.duplicate_customers_by_email as
    select
        lower(email) as email_lower,
        count(*)::int as duplicate_count,
        min(signup_date) as earliest_signup_date,
        array_agg(customer_id order by signup_date, customer_id) as customer_ids
    from landing.customers
    group by lower(email)
    having count(*) > 1
    order by duplicate_count desc, email_lower;

    """
    ,
    """
    create or replace view structured.orders_missing_customers as
    select
        o.order_id,
        o.customer_id,
        o.order_ts,
        o.status,
        o.total_amount,
        o.currency
    from landing.orders o
    left join landing.customers c
    on c.customer_id = o.customer_id
    where c.customer_id is null
    order by o.order_ts;
    """
,
]

def create_views():
    with get_conn() as conn:
        with conn.cursor() as cur:
            
            for stmt in sql:
                cur.execute(stmt)
                print(stmt.strip().splitlines()[0], "OK")
                
if __name__ == "__main__":
    create_views()   
    
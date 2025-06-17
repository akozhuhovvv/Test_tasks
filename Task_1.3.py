import pandas as pd
from sqlalchemy import create_engine

# подключаемся к БД
ENGINE_URI = "postgresql+psycopg2://user:password@localhost:5432/shop"

def build_customer_summary() -> pd.DataFrame:
    engine = create_engine(ENGINE_URI, pool_pre_ping=True)

    sql = """
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(DISTINCT o.order_id)                           AS total_orders,
            COALESCE(SUM(oi.quantity * oi.unit_price), 0)::NUMERIC(14,2) AS total_spent,
            MAX(o.order_date)                                    AS last_order_date
        FROM customers        AS c
        LEFT JOIN orders      AS o  ON o.customer_id = c.customer_id
        LEFT JOIN order_items AS oi ON oi.order_id   = o.order_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY c.customer_id;
    """

    df = pd.read_sql_query(sql, engine)
    return df


if __name__ == "__main__":
    summary_df = build_customer_summary()
    print(summary_df)
    # можно материализовать в БД:
    # summary_df.to_sql("customer_summary", engine, if_exists="replace", index=False)
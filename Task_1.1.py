import pandas as pd
from sqlalchemy import create_engine, text

# подключаемся к БД
ENGINE_URI = "postgresql+psycopg2://user:password@localhost:5432/shop"
CSV_PATH = "orders_new.csv"


def main() -> None:
    engine = create_engine(ENGINE_URI, pool_pre_ping=True)

    # 1. читаем CSV (пробел/таб-разделитель)
    df = pd.read_csv(CSV_PATH, sep=r"\s+", engine="python")

    # 2. фильтруем по существующим клиентам и убираем дубли по order_id
    with engine.begin() as conn:
        valid_customers = {r[0] for r in conn.execute(text("SELECT customer_id FROM customers"))}
        existing_orders = {r[0] for r in conn.execute(text("SELECT order_id   FROM orders"))}

    df = df[df["customer_id"].isin(valid_customers) & ~df["order_id"].isin(existing_orders)]

    # 3. пишем в таблицу orders
    if not df.empty:
        df.to_sql("orders",
                  engine,
                  if_exists="append",
                  index=False,
                  method="multi",
                  chunksize=1_000)
        print(f"✅ Inserted {len(df)} orders")
    else:
        print("ℹ️ Нет подходящих новых заказов")

if __name__ == "__main__":
    main()



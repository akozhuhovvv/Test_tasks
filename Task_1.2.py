import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Union


def load_orders_from_csv(path_to_csv: str, db_conn: Union[str, Engine]) -> None:

    #1. создаём SQLAlchemy-engine
    engine = db_conn if isinstance(db_conn, Engine) else create_engine(db_conn)

    #2. читаем CSV (пробел/таб-разделитель)
    df = pd.read_csv(path_to_csv, sep=r"\s+", engine="python")

    #3. оставляем строки с существующими customer_id
    with engine.begin() as cn:
        valid_customers = {row[0] for row in cn.execute(text("SELECT customer_id FROM customers"))}
    df = df[df["customer_id"].isin(valid_customers)]

    #4. загружаем в таблицу orders
    if not df.empty:
        df.to_sql("orders", engine, if_exists="append", index=False)
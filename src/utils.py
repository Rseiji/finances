import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
import os

PWD = os.getenv("postgres_pwd")
CONN_STR = f"postgresql+psycopg2://postgres:rafael26@localhost:5432/finances"


def persist_dataframe_to_database(
    df: pd.DataFrame,
    schema: str,
    table: str,
    assign_processed_at_column: bool = False,
    conn_str: str = CONN_STR,
    saving_kwargs: dict | None = None,
    upsert: bool = True,
    pk_columns: list[str] | None = None,
) -> None:
    if saving_kwargs is None:
        saving_kwargs = {}

    if assign_processed_at_column:
        df["_processed_at"] = datetime.now()

    engine = create_engine(conn_str)
    with engine.begin() as conn:
        if upsert:
            from sqlalchemy import Table, MetaData
            meta = MetaData(schema=schema)
            t = Table(table, meta, autoload_with=engine)
            records = df.to_dict(orient="records")
            for record in records:
                stmt = pg_insert(t).values(**record)
                update_cols = {c: stmt.excluded[c] for c in df.columns if c not in pk_columns}
                stmt = stmt.on_conflict_do_update(index_elements=pk_columns, set_=update_cols)
                conn.execute(stmt)
        else:
            df.to_sql(
                name=table,
                schema=schema,
                con=engine,
                if_exists="append",
                index=False,
                **saving_kwargs
            )


def read_sql_query(query: str, conn_str: str = CONN_STR) -> pd.DataFrame:
    """Execute an SQL query and returns the result as a pandas DataFrame.

    Params
    ------
    query (str)
        The SQL query to execute.
    conn_str (str)
        The connection string for the PostgreSQL database.

    Returns
    -------
    pd.DataFrame
        The result of the query as a DataFrame.
    """
    engine = create_engine(conn_str)
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection)

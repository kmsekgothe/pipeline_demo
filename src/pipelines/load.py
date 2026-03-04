import io
import pandas as pd
from src.db_utils.db_connect import get_conn

def copy_df_into_table(df: pd.DataFrame, table: str, columns: list[str]) -> None:
    if df.empty:
        return

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols_sql = ", ".join(columns)
    sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV)"

    with get_conn() as conn:
        with conn.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(buf.getvalue())
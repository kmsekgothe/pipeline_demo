import os
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg

load_dotenv()

def conninfo() -> str:
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    db   = os.getenv("PGDATABASE")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

@contextmanager
def get_conn():
    with psycopg.connect(conninfo()) as conn:
        # psycopg3 uses transaction blocks by default; commit/rollback is handled by context manager
        yield conn
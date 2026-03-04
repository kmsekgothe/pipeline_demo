from pathlib import Path
import pandas as pd
import json
import yaml
# from src.utils.logging import get_logger
# from src.utils.validation import require_columns
# from src.db.load import load_raw_payload

# log = get_logger("ingest")
def load_config(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    try:
        if suffix == ".csv":
            return pd.read_csv(path)

        if suffix == ".jsonl":
            rows = []
            with open(path, "r") as f:
                for i, line in enumerate(f, start=1):
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Invalid JSON at line {i} in {path}: {e} ---- {line}")
            return pd.DataFrame(rows)

        raise ValueError(f"Unsupported file type: {suffix}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error reading {path}: {e}")

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make columns consistent: strip spaces, lowercase, replace spaces with underscores.
    Keeps it simple; deeper cleaning comes later in transform/validate.
    """
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df

def ingest(config_path: Path) -> dict[str, pd.DataFrame]:
    """
    Reads config.yaml and returns:
        { table_name: DataFrame }
    """
    config = load_config(config_path)

    dataframes: dict[str, pd.DataFrame] = {}

    for table_name, file_path in config["tables"].items():
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        df = read_file(path)
        dataframes[table_name] = df
    #print(dataframes)
    return dataframes
    
    
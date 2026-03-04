from __future__ import annotations

from typing import Dict, Tuple
import pandas as pd
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
def _lowercase_email(df: pd.DataFrame, col: str = "email") -> pd.DataFrame:
    if col in df.columns:
        df[col] = df[col].astype("string").str.strip().str.lower()
    return df


import pandas as pd

def _to_utc(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Goal:
      - If tz-aware: convert to UTC
      - If tz-naive: assume it's UTC (do NOT lose data)
      - If initial parse fails for some formats: try fallback formats
    """
    for c in cols:
        if c not in df.columns:
            continue

        s = df[c]

        # 1) First pass: handles tz-aware and naive; naive assumed UTC
        dt = pd.to_datetime(s, errors="coerce", utc=True)

        # 2) Fallback for rows that failed parsing (NaT)
        mask = dt.isna() & s.notna()
        if mask.any():
            # Try a common explicit format first (your example: "2024/03/04 12:00:00")
            dt2 = pd.to_datetime(s[mask], errors="coerce", format="%Y/%m/%d %H:%M:%S")
            # If still NaT, try a more flexible parse (slashes -> dashes helps)
            still = dt2.isna() & s[mask].notna()
            if still.any():
                cleaned = s[mask][still].astype(str).str.replace("/", "-", regex=False)
                dt3 = pd.to_datetime(cleaned, errors="coerce")
                dt2.loc[still] = dt3

            # Localize fallback-naive datetimes to UTC (date-only/naive -> UTC midnight/time)
            # dt2 may still contain NaT for truly invalid strings
            dt.loc[mask] = dt2.dt.tz_localize("UTC")

        df[c] = dt

    return df


def _cast_numeric(df: pd.DataFrame, casts: dict[str, str]) -> pd.DataFrame:
    """
    casts example: {"quantity": "Int64", "unit_price": "float64"}
    Invalid casts -> NA (for nullable Int64) or NaN.
    """
    for c, dtype in casts.items():
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            # pandas nullable ints need explicit astype
            df[c] = df[c].astype(dtype) if dtype.lower().startswith("int") else df[c].astype(dtype)
    return df
def _deduplicate_email(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase emails and keep earliest signup per email.
    Assumes:
        - email column exists
        - created_at column exists
    """

    if "email" not in df.columns:
        return df

    # Ensure datetime is parsed
    if "signup_date" in df.columns:
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.date

        # Sort by signup date ascending
        df = df.sort_values("signup_date")

    # rop duplicates, keep first occurrence
    df = df.drop_duplicates(subset=["email"], keep="first")

    return df

def _normalize_status(
    df: pd.DataFrame,
    col: str = "status",
    mapping: dict[str, str] | None = None,
    allowed: set[str] | None = None,
    default: str | None = None,
    drop_invalid: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Maps/cleans status values.
    If drop_invalid=True -> removes invalid rows into rejects.
    Else -> sets invalid statuses to default (must be provided).
    Returns: (clean_df, rejected_df)
    """
    if col not in df.columns:
        return df, df.iloc[0:0].copy()

    s = df[col].astype("string").str.strip().str.lower()

    if mapping:
        s = s.map(lambda x: mapping.get(x, x))

    if allowed:
        valid_mask = s.isin(list(allowed))
    else:
        valid_mask = pd.Series([True] * len(df), index=df.index)

    if drop_invalid:
        rejected = df.loc[~valid_mask].copy()
        clean = df.loc[valid_mask].copy()
        clean[col] = s.loc[valid_mask]
        return clean, rejected

    # map invalid -> default
    if default is None:
        raise ValueError("default must be provided when drop_invalid=False")

    s = s.where(valid_mask, other=default)
    df[col] = s
    return df, df.iloc[0:0].copy()


def _handle_nonpositive_quantity(
    df: pd.DataFrame,
    col: str = "quantity",
    mode: str = "clip",  # "drop" or "clip"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    mode="drop": remove rows with qty <= 0 (into rejects)
    mode="clip": set qty <= 0 to 1 (or 0 if you prefer)
    Returns: (clean_df, rejected_df)
    """
    if col not in df.columns:
        return df, df.iloc[0:0].copy()

    qty = pd.to_numeric(df[col], errors="coerce")
    bad_mask = qty.isna() | (qty <= 0)

    if mode == "drop":
        rejected = df.loc[bad_mask].copy()
        clean = df.loc[~bad_mask].copy()
        clean[col] = pd.to_numeric(clean[col], errors="coerce")
        return clean, rejected

    if mode == "clip":
        df[col] = qty.abs()
        return df, df.iloc[0:0].copy()

    raise ValueError("mode must be 'drop' or 'clip'")
import re
import pandas as pd


def validate_email(df: pd.DataFrame, col: str = "email"):
    """
    Drops rows with invalid email format.
    Returns: (clean_df, rejected_df)
    """
    EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    if col not in df.columns:
        return df, df.iloc[0:0].copy()

    # Normalize first
    df[col] = df[col].astype("string").str.strip().str.lower()

    valid_mask = df[col].str.match(EMAIL_REGEX, na=False)

    clean_df = df.loc[valid_mask].copy()
    rejected_df = df.loc[~valid_mask].copy()

    return clean_df, rejected_df



def reject_orders_missing_customers(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    orders_customer_col: str = "customer_id",
    customers_key_col: str = "customer_id",
    ):
    valid_customer_ids = set(customers[customers_key_col].dropna().unique())

    mask = orders[orders_customer_col].isin(valid_customer_ids)

    clean_orders = orders.loc[mask].copy()
    rejected_orders = orders.loc[~mask].copy()

    return clean_orders, rejected_orders

def transform(ingested: Dict[str, pd.DataFrame]) -> tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Input:  {"landing.customer": df, "landing.orders": df, ...} or {"customer": df, ...}
    Output: (cleaned_dfs, rejected_rows_by_table)

    This keeps it simple: each table transform is explicit and easy to review.
    """
    cleaned: Dict[str, pd.DataFrame] = {}
    rejects: Dict[str, pd.DataFrame] = {}
    customers = ''
    orders = ''
    
    for table, df0 in ingested.items():
        df = df0.copy()
        rej_all = []

        # ---- CUSTOMER ----
        if "customers" in table:
            df = _lowercase_email(df, col="email") 
            df = _deduplicate_email(df)
            
            df, rej_email = validate_email(df, col="email")
            rej_all.append(rej_email)
            
        # ---- ORDERS ----
        elif "orders" in table:
            
            df = _to_utc(df, cols=["order_ts"])

            df, rej = _normalize_status(
                df,
                col="status",
                mapping={"complete": "placed", "processing": "shipped"},
                allowed={"placed", "refunded", "cancelled","shipped"},
                default="invalid",
                drop_invalid=True,
            )
            rej_all.append(rej)
            df, rej_missing_customer = reject_orders_missing_customers(df, customers)
            
            rej_all.append(rej_missing_customer)
            df = _cast_numeric(df, casts={"total_amount": "float64"})
            orders = df
        # ---- ORDER ITEMS ----
        elif "order_items" in table or "items" in table:
            df = _cast_numeric(df, casts={"quantity": "Int64", "unit_price": "float64"})

            df, rej_qty = _handle_nonpositive_quantity(df, col="quantity", mode="drop")
            
            rej_all.append(rej_qty)
            
            df, rej_price = _handle_nonpositive_quantity(df, col="unit_price", mode="clip")

            rej_all.append(rej_price)
            df, rej_missing_customer = reject_orders_missing_customers(df, orders,"order_id","order_id")
            
            rej_all.append(rej_missing_customer)
        # Default: still apply generic datetime/email if present
        else:
            df = _lowercase_email(df, col="email")
            df = _to_utc(df, cols=[c for c in ["created_at", "updated_at"] if c in df.columns])

        # combine rejects for this table
        valid_rejects = [r for r in rej_all if r is not None and len(r) > 0]

        if valid_rejects:
            rej_table = pd.concat(valid_rejects, ignore_index=True)
        else:
            rej_table = df.iloc[0:0].copy()
            
        cleaned[table] = df
        customers = cleaned["customers"]
        
        rejects[table] = rej_table
    #print(cleaned)
    return cleaned, rejects
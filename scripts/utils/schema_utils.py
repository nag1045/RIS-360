import pandas as pd


def clean_numeric(series):
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace(['N/R', 'n/r', '-', '', 'None', 'nan'], None)
    )
    return pd.to_numeric(cleaned, errors='coerce')

def clean_boolean(series,col):

    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
    )

    mapping = {
        # True values
        'yes': 1,
        'y': 1,
        'true': 1,
        '1': 1,
        '1.0': 1,

        # False values
        'no': 0,
        'n': 0,
        'false': 0,
        '0': 0,
        '0.0': 0,

        # Missing values
        'n/r': None,
        '-': None,
        '': None,
        'none': None,
        'nan': None
    }

    mapped = cleaned.map(mapping)

    # Detect unexpected values
    unexpected = cleaned[~cleaned.isin(mapping.keys()) & cleaned.notna()]

    if len(unexpected) > 0:
        print(f"⚠ Unexpected boolean values in column '{col}':")
        print(unexpected.unique()[:10])

    return mapped.astype("Int64")

    return pd.to_numeric(cleaned, errors='coerce').astype("Int64")

def safe_int_cast(series, column_name):

    # Convert to string and strip whitespace
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace(['N/R', 'n/r', '-', '', 'None', 'nan'], None)
    )

    # Convert to numeric
    numeric = pd.to_numeric(cleaned, errors='coerce')

    # Check for non-integer decimal values
    if not ((numeric.dropna() % 1 == 0).all()):
        print(f"⚠ WARNING: Column '{column_name}' contains non-integer values.")
        print("Keeping as float to prevent data corruption.")
        return numeric

    return numeric.astype("Int64")

def clean_date(series):
    return pd.to_datetime(series, errors='coerce')


def enforce_schema(df, schema):

    for col in schema.get("float", []):
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    for col in schema.get("int", []):
        if col in df.columns:
            df[col] = safe_int_cast(df[col], col)
            

    for col in schema.get("boolean", []):
        if col in df.columns:
            df[col] = clean_boolean(df[col],col)

    for col in schema.get("date", []):
        if col in df.columns:
            df[col] = clean_date(df[col])

    return df

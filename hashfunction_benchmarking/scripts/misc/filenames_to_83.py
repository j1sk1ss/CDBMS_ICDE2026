import re
import pandas as pd

names_path: str = "dataset_names_padded.csv"
extensions_path: str = "dataset_extensions_padded.csv"

def is_cyrillic(s) -> bool:
    return bool(re.search(r'[А-Яа-яЁё]', s))

def digits_ratio(s: str) -> float:
    digits = sum(c.isdigit() for c in s)
    return digits / len(s) if s else 0

def clean_and_pad_column(df: pd.DataFrame, column: str, padd: int) -> pd.DataFrame:
    df = df.dropna(subset=[column])
    df = df[~df[column].str.fullmatch(r'\d+')]
    df = df.drop_duplicates(subset=[column])
    
    def is_valid(s: str) -> bool:
        if s.isdigit():
            return False
        if is_cyrillic(s):
            return False
        if digits_ratio(s) >= 0.5:
            return False
        if not re.fullmatch(r'[A-Za-z0-9._\- ]+', s):
            return False
        return True
    
    df = df[df[column].apply(is_valid)]
    
    df[column] = df[column].astype(str)
    df[column] = df[column].str[:padd]
    df[column] = df[column].str.ljust(padd)
    return df

df_names: pd.DataFrame = pd.read_csv(names_path)
df_exts: pd.DataFrame = pd.read_csv(extensions_path)
df_names_cleaned: pd.DataFrame = clean_and_pad_column(df_names, "name", 8)
df_exts_cleaned: pd.DataFrame = clean_and_pad_column(df_exts, "extension", 3)
df_names_cleaned.to_csv(names_path, index=False)
df_exts_cleaned.to_csv(extensions_path, index=False)

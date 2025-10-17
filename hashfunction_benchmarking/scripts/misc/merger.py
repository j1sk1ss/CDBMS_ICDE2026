import pandas as pd

def merge_csv_unique(f: str, s: str, column: str, output_file: str) -> None:
    df1 = pd.read_csv(f)
    df2 = pd.read_csv(s)
    if column not in df1.columns or column not in df2.columns:
        raise ValueError("Column not found!")

    combined = pd.concat([df1[[column]], df2[[column]]], ignore_index=True)
    combined = combined.drop_duplicates(subset=[column])
    combined.to_csv(output_file, index=False)
    
merge_csv_unique("dataset_extensions.csv", "dataset_extensions_padded.csv", "extension", "dataset_extensions_padded.csv")
merge_csv_unique("dataset_names.csv", "dataset_names_padded.csv", "name", "dataset_names_padded.csv")

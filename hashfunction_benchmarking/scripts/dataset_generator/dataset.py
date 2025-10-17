from dataclasses import dataclass
import pandas as pd

@dataclass
class DatasetInfo:
    dataset: pd.DataFrame
    vocab: set
    max_len: int
    latent_dim: int
    
    def to_dict(self) -> dict:
        return {
            "dataset": "YES" if self.dataset.empty else "NO",
            "vocab": self.vocab,
            "max_len": self.max_len,
            "latent_dim": self.latent_dim
        }

class DataManager:
    @staticmethod
    def get_dataset_info(column: str, path: str) -> DatasetInfo:
        df: pd.DataFrame = pd.read_csv(path)
        all_text: list[str] = sorted(''.join(set(df[column].tolist())).lower())
        return DatasetInfo(
            dataset=df, vocab=set(all_text), max_len=df[column].str.len().max(), latent_dim=32
        )
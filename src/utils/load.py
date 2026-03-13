import pandas as pd
import yaml
from pathlib import Path

def _find_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / '.root').exists():
            return parent
    raise FileNotFoundError("Could not find project root (.root not found)")

def load(path: str):
    """load files

    Args:
        path (str): file path relative to project root

    Returns:
        object: based on file type
    """
    ROOT = _find_root()
    loc = ROOT / path
    ext = path.split(sep=".")[-1]

    try:
        match ext:
            case "csv":
                obj = pd.read_csv(loc)
            case "parquet":
                obj = pd.read_parquet(loc)
            case "pkl":
                obj = pd.read_pickle(loc)
            case "yaml" | "yml":
                obj = yaml.safe_load(open(loc, encoding='utf-8', mode='r'))

            case _:
                raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        print(f"Error loading file: {e}")
        raise e
    
    return obj

def dump(path: str):
    ROOT = _find_root()
    loc = ROOT / path
    try:
        Path.mkdir(loc, exist_ok=True, parents=True)
        
    except Exception as e:
        print(e)
        raise e
    return
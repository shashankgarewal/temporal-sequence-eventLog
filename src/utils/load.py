import pandas as pd
import yaml
from pathlib import Path
import os
from dotenv import load_dotenv

import logging
logger = logging.getLogger(__name__)

def _find_root() -> Path:
    # for package
    load_dotenv()
    if env := os.environ.get("EVENTLOGS_ROOT"):
        return Path(env)
    
    # for local director and repo
    OPTIONS = ('.git', '.root', 'setup.py', 'requirements.txt')
    for parent in Path(__file__).resolve().parents:
        if any((parent / marker).exists() for marker in OPTIONS):
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
    ext = path.split(sep=".")[-1].lower()

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
        logger.info(f"Error loading file: {e}")
        raise e
    
    return obj

def dump(obj, path: str):
    ROOT = _find_root()
    loc = ROOT / path
    ext = loc.suffix.replace(".", "").lower()
    # check object type and file compatibility
    
    try:
        loc.parent.mkdir(exist_ok=True, parents=True)
    except Exception as e:
        logger.info(f"Error creating directory: {e}")
        raise e

    try:
        match ext:
            case "csv":
                obj.to_csv(loc, index=False)
            case "parquet":
                obj.to_parquet(loc, engine='pyarrow')
            case "pkl":
                obj.to_pickle(loc)
            case "yaml" | "yml":
                with open(loc, 'w', encoding='utf-8') as f:
                    yaml.dump(obj, f, default_flow_style=False)
            case _:
                raise ValueError(f"Unsupported save format: {ext}")
                
        logger.info(f"Successfully saved to: {path}")        
    except Exception as e:
        logger.info(f"Error saving file: {e}")
        raise e
    return
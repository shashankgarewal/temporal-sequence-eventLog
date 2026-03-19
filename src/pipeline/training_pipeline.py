# app.py
"""Pipeline entry point — runs staging, canonical, and base feature builds in sequence."""

from src.data.staging import build_staging
from src.data.canonical import build_canonical_events
from src.data.base_features import build_base_features

import warnings
warnings.filterwarnings("ignore")

import logging
logger = logging.getLogger(__name__)

# for development
logging.basicConfig(level=logging.INFO, format='%(message)s')

# for production - only critical logs
# logging.basicConfig(level=logging.WARNING, format='%(message)s') 

if __name__ == "__main__":
    
    build_staging()
    build_canonical_events()
    df = build_base_features()

# generated_dir/__init__.py
import sys
from pathlib import Path

# Dynamically adds this specific folder to the Python path
sys.path.append(str(Path(__file__).parent.resolve()))
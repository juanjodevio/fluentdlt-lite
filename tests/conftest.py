"""Root-level pytest configuration for test imports."""

import sys
import importlib.util
from pathlib import Path

# Add src directory to Python path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Create fldt module alias by loading src as fldt
# This allows 'from fldt import ...' to work when the package is in src/
spec = importlib.util.spec_from_file_location("fldt", src_path / "__init__.py")
if spec and spec.loader:
    fldt_module = importlib.util.module_from_spec(spec)
    # Set __path__ to allow relative imports to work
    fldt_module.__path__ = [str(src_path)]
    sys.modules["fldt"] = fldt_module
    spec.loader.exec_module(fldt_module)

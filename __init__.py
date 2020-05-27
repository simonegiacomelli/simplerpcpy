import sys
from pathlib import Path


def add_sys_path(p):
    if p not in sys.path:
        sys.path.insert(0, p)


add_sys_path(str(Path(__file__).parent)+'/src')
print(sys.path)

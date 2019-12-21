import os
import pkg_resources

PY_DIR = os.path.abspath(os.path.dirname(__file__))

SQL_DIR = os.path.abspath(os.path.join(PY_DIR, 'sql'))

TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "test"))

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))

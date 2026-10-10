"""Check that local database settings are not embedded in source."""

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "Stockage et structuration des données de ventes Superstore avec PostgreSQL et SQLAlchemy.py"


class SecurityContractTests(unittest.TestCase):
    def test_source_uses_environment_for_database_access(self):
        source = SCRIPT.read_text(encoding="utf-8")
        compile(source, str(SCRIPT), "exec")
        tree = ast.parse(source)
        self.assertIn("URL.create(", source)
        self.assertIn('os.environ["DB_PASSWORD"]', source)
        self.assertNotIn('DB_PASS = "', source)
        self.assertNotIn("DB_PASSWORD = '", source)
        self.assertFalse(any(ord(ch) >= 0x1F300 for ch in source))
        self.assertIsInstance(tree, ast.Module)

    def test_gitignore_covers_secrets(self):
        self.assertIn("*.env", (ROOT / ".gitignore").read_text())
        self.assertIn("replace_with_local_password", (ROOT / ".env.example").read_text())


if __name__ == "__main__":
    unittest.main()

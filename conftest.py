import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
TEST_DB = Path('/tmp/flexuni_pytest.db')
try:
    TEST_DB.unlink()
except FileNotFoundError:
    pass
os.environ['DATABASE_URL'] = 'sqlite:///' + str(TEST_DB)
os.environ['AUTO_CREATE_DB'] = '1'
os.environ['SECRET_KEY'] = 'test-secret'
os.environ['PAYMENT_WEBHOOK_SECRET'] = 'webhook-secret'

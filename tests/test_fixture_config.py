import pytest
import sys
sys.path = ['/home/bman7777/local-binary/lamplight',
            '/home/bman7777/local-binary'] + sys.path
from __init__ import app

@pytest.fixture(scope='session', autouse=True)
def client():
    app.config['TESTING'] = True
    client = app.test_client()

    yield client

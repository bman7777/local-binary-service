import pytest
import json
from tests.test_fixture_config import client

def test_allowed(client):
    response = client.put('/search')
    assert response.status_code == 405

    response = client.get('/search')
    assert response.status_code == 405

    response = client.delete('/search')
    assert response.status_code == 405

@pytest.mark.parametrize("param", [
        [
            {
                "english": {"words": ["hope"]},
                "native": {"concords": [24114]}
            }
        ],
        [
            {
                "english": {"words": ["hope"]}
            }
        ],
        [
            {
                "native": {"concords": [24635]}
            }
        ],
    ])
def test_standard(client, param):
    response = client.post('/search', data=json.dumps(param),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) > 0

def test_nodata(client):
    response = client.post('/search', data=json.dumps([]),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 204
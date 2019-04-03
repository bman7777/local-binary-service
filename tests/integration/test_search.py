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
        [
            {
                "english": {"words": ["the Day of the lord"]}
            }
        ],
        [
            {
                "english": {"words": ["the Day of the lord"]}
            },
            {
                "native": {"concords": [29938]}
            }
        ],
        [
            {
                "english": {"words": ["Romans 8:28"]}
            },
            {
                "english": {"words": ["good"]}
            },
        ],
        [
            {
                "english": {"words": ["Romans 8:28"]}
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

@pytest.mark.parametrize("param", [
        [
            {
                "english": {"words": ["hope"], "synonym": True},
                "native": {"concords": [24114]}
            }
        ],
        [
            {
                "english": {"words": ["hope"], "synonym": True}
            }
        ],
    ])
def test_synonym(client, param):
    response = client.post('/search', data=json.dumps(param),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) > 0

@pytest.mark.parametrize("param", [
        [
            {
                "english": {"words": ["Jesus"]}
            }
        ],
    ])
def test_stats(client, param):
    response = client.post('/search', data=json.dumps(param),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) > 0

    for stat_item in out["stats"]:
        if stat_item['type'] == 'tabs':
            assert len(stat_item["data"]) >= 4
            break

@pytest.mark.parametrize("param", [
        [
            {
                "english": {"words": ["hope"]}, "similar": True, "synonym": False
            },
            {
                "english": {"words": ["fear"]}, "similar": True, "synonym": False
            }
        ],
    ])
def test_nummatches(client, param):
    response = client.post('/search', data=json.dumps(param),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) > 0

    for stat_item in out["stats"]:
        if stat_item['type'] == 'stat':
            assert stat_item["data"][0]["value"] == len(out["data"])
            print("values is: "+str(stat_item["data"][0]["value"]))
            break

@pytest.mark.parametrize("param", [
        [
            {
                "english": {"words": ["hope"]},
            },
        ],
    ])
def test_bookfilter(client, param):
    response = client.post('/search?book=Matthew', data=json.dumps(param),
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) > 0

import pytest
import json
from urllib.parse import quote
from tests.test_fixture_config import client

def test_allowed(client):
    response = client.put('/baseless-search')
    assert response.status_code == 405

    response = client.post('/baseless-search')
    assert response.status_code == 405

    response = client.delete('/baseless-search')
    assert response.status_code == 405

@pytest.mark.parametrize("good_verse", [
        ("Romans 8:28", "Romans", 8, 28),
        ("      Romans 8:28", "Romans", 8, 28),
        ("Romans 8:28       ", "Romans", 8, 28),
        ("Romans     8:28", "Romans", 8, 28),
        ("Romans8:28", "Romans", 8, 28),
        ("Romans8:28abc", "Romans", 8, 28),
        ("Romans 8.0:28", "Romans", 8, 28),
        ("Rmns 8:28", "Romans", 8, 28),
        ("rmns 8:28", "Romans", 8, 28),
        ("2 Timothy 4:7", "2 Timothy", 4, 7),
        ("2Tim4:7", "2 Timothy", 4, 7),
        ("Psalms 140:6", "Psalms", 140, 6),
    ])
def test_standard(client, good_verse):
    response = client.get('/baseless-search?text='+quote(good_verse[0]))
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) == 1
    assert out["data"][0]["book"] == good_verse[1]
    assert out["data"][0]["chapter"] == good_verse[2]
    assert out["data"][0]["verse"] == good_verse[3]
    assert out["data"][0]["text"]


@pytest.mark.parametrize("con", [
        (22254, 200),
        (-1, 204),
    ])
def test_concord_standard(client, con):
    response = client.get('/baseless-search?concord='+str(con[0]))
    assert response.status_code == con[1]


@pytest.mark.parametrize("bad_verse", [
        "Romans 0:7",
        "Romans -1:7",
        "Romans 7:0",
        "Romans 17:10",
        "Romans 7.8:4",
        "Wrong 7:7",
        ""
    ])
def test_bad_verse(client, bad_verse):
    response = client.get('/baseless-search?text='+quote(bad_verse))
    assert response.status_code == 204


@pytest.mark.parametrize("word", [
        "worlds",
        "WORlds",
        "House",
        "house",
        "Jesus",
    ])
def test_word_one_concord(client, word):
    response = client.get('/baseless-search?text='+word)
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) >= 1
    assert out["data"][0]["type"]
    assert out["data"][0]["lang"]
    assert out["data"][0]["native"]
    assert out["data"][0]["english"]
    assert out["data"][0]["description"]


@pytest.mark.parametrize("word", [
        "Holy Spirit",
        "HOLy SpiRIt"
    ])
def test_phrase_one_concord(client, word):
    response = client.get('/baseless-search?text='+word)
    assert response.status_code == 200
    out = json.loads(response.data)
    assert len(out["data"]) >= 1
    assert out["data"][0]["book"]
    assert out["data"][0]["chapter"]
    assert out["data"][0]["verse"]
    assert out["data"][0]["text"]


@pytest.mark.parametrize("word", [
        "hope",
    ])
def test_word_synonyms(client, word):
    response = client.get('/baseless-search?text='+word+'&synonym')
    assert response.status_code == 200
    out = json.loads(response.data)
    num_with = len(out["data"])
    assert num_with >= 1
    assert out["data"][0]["type"]
    assert out["data"][0]["lang"]

    response = client.get('/baseless-search?text='+word)
    assert response.status_code == 200
    out = json.loads(response.data)
    assert num_with > len(out["data"]) or out["next_page"]
    assert out["data"][0]["type"]
    assert out["data"][0]["lang"]

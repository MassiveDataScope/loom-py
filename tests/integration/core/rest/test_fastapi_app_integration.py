from __future__ import annotations

import base64
import json
from pathlib import Path

import prometheus_client
import pytest
from fastapi.testclient import TestClient

from tests.integration.fake_repo.main import create_app_from_config


@pytest.mark.parametrize(
    "config_relpath",
    [
        "tests/integration/fake_repo/config/conf.modules.yaml",
        "tests/integration/fake_repo/config/conf.manifest.yaml",
    ],
)
def test_fake_repo_app_bootstrap_without_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    config_relpath: str,
) -> None:
    db_path = tmp_path / "fake-repo-app.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    config_path = Path(config_relpath)
    app = create_app_from_config(
        str(config_path),
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/products/",
            json={"name": "  keyboard  ", "price": 120.0},
        )
        assert create_response.status_code == 201
        assert "x-request-id" in create_response.headers
        created = create_response.json()
        assert created["id"] == 1
        assert created["name"] == "keyboard"

        by_name_response = client.get("/products/by-name/keyboard")
        assert by_name_response.status_code == 200
        assert by_name_response.json()["id"] == 1

        list_response = client.get("/products/")
        assert list_response.status_code == 200
        listed = list_response.json()
        assert listed["totalCount"] == 1
        assert listed["items"][0]["id"] == 1

        get_response = client.get("/products/1")
        assert get_response.status_code == 200
        loaded = get_response.json()
        assert loaded is not None
        assert loaded["id"] == 1
        assert loaded["name"] == "keyboard"

        update_response = client.patch("/products/1", json={"price": 99.9})
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated is not None
        assert updated["price"] == pytest.approx(99.9)

        delete_response = client.delete("/products/1")
        assert delete_response.status_code == 200
        assert delete_response.json() is True

        missing_response = client.get("/products/1")
        assert missing_response.status_code == 200
        assert missing_response.json() is None

        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 404


def test_fake_repo_app_bootstrap_with_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "fake-repo-app.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    config_path = Path("tests/integration/fake_repo/config/conf.interfaces.yaml")
    registry = prometheus_client.CollectorRegistry()
    app = create_app_from_config(str(config_path), metrics_registry=registry)

    with TestClient(app) as client:
        create_response = client.post(
            "/products/",
            json={"name": "  keyboard  ", "price": 120.0},
        )
        assert create_response.status_code == 201
        assert "x-request-id" in create_response.headers
        created = create_response.json()
        assert created["id"] == 1
        assert created["name"] == "keyboard"

        by_name_response = client.get("/products/by-name/keyboard")
        assert by_name_response.status_code == 200
        assert by_name_response.json()["id"] == 1

        list_response = client.get("/products/")
        assert list_response.status_code == 200
        listed = list_response.json()
        assert listed["totalCount"] == 1
        assert listed["items"][0]["id"] == 1

        get_response = client.get("/products/1")
        assert get_response.status_code == 200
        loaded = get_response.json()
        assert loaded is not None
        assert loaded["id"] == 1
        assert loaded["name"] == "keyboard"

        update_response = client.patch("/products/1", json={"price": 99.9})
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated is not None
        assert updated["price"] == pytest.approx(99.9)

        delete_response = client.delete("/products/1")
        assert delete_response.status_code == 200
        assert delete_response.json() is True

        missing_response = client.get("/products/1")
        assert missing_response.status_code == 200
        assert missing_response.json() is None

        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 200
        assert "http_requests_total" in metrics_response.text
        assert "http_request_duration_seconds" in metrics_response.text

        metrics_trailing_slash_response = client.get("/metrics/")
        assert metrics_trailing_slash_response.status_code == 404


@pytest.mark.parametrize(
    "config_relpath",
    [
        "tests/integration/fake_repo/config/conf.modules.yaml",
        "tests/integration/fake_repo/config/conf.manifest.yaml",
    ],
)
def test_fake_repo_list_query_cursor_and_profile_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    config_relpath: str,
) -> None:
    db_path = tmp_path / "fake-repo-query.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    app = create_app_from_config(config_relpath)
    with TestClient(app) as client:
        assert client.post("/products/", json={"name": "a", "price": 10.0}).status_code == 201
        assert client.post("/products/", json={"name": "b", "price": 20.0}).status_code == 201
        assert client.post("/products/", json={"name": "c", "price": 20.0}).status_code == 201

        offset_response = client.get("/products/?price__eq=20&limit=1&page=1&sort=id&direction=ASC")
        assert offset_response.status_code == 200
        offset_payload = offset_response.json()
        assert offset_payload["totalCount"] == 2
        assert len(offset_payload["items"]) == 1
        assert offset_payload["items"][0]["name"] == "b"

        cursor_first = client.get(
            "/products/?price__eq=20&pagination=cursor&limit=1&sort=id&direction=ASC"
        )
        assert cursor_first.status_code == 200
        cursor_first_payload = cursor_first.json()
        assert cursor_first_payload["hasNext"] is True
        assert cursor_first_payload["nextCursor"] is not None
        assert cursor_first_payload["items"][0]["name"] == "b"

        next_cursor = cursor_first_payload["nextCursor"]
        cursor_second = client.get(
            f"/products/?price__eq=20&pagination=cursor&after={next_cursor}&limit=1&sort=id&direction=ASC"
        )
        assert cursor_second.status_code == 200
        cursor_second_payload = cursor_second.json()
        assert cursor_second_payload["hasNext"] is False
        assert cursor_second_payload["nextCursor"] is None
        assert cursor_second_payload["items"][0]["name"] == "c"

        get_with_profile = client.get("/products/2?profile=with_details")
        assert get_with_profile.status_code == 200
        details_payload = get_with_profile.json()
        assert "hasReviews" in details_payload
        assert "countReviews" in details_payload
        assert "reviewSnippets" in details_payload

        get_invalid_profile = client.get("/products/2?profile=invalid")
        assert get_invalid_profile.status_code == 400

        post_profile_forbidden = client.post(
            "/products/?profile=with_details",
            json={"name": "forbidden", "price": 1.0},
        )
        assert post_profile_forbidden.status_code == 400


@pytest.mark.parametrize("sort", ["name", "no_such_column"])
def test_fake_repo_list_refuses_a_sort_on_an_undeclared_field_alike(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sort: str
) -> None:
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'sort.sqlite'}")
    app = create_app_from_config("tests/integration/fake_repo/config/conf.modules.yaml")
    with TestClient(app) as client:
        assert client.post("/products/", json={"name": "s3cr3t", "price": 1.0}).status_code == 201
        assert client.post("/products/", json={"name": "other", "price": 2.0}).status_code == 201

        response = client.get(f"/products/?sort={sort}&pagination=cursor&limit=1")

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "unsupported_query"
    assert detail["message"] == f"Sorting on field {sort!r} is not allowed by this route."
    assert "nextCursor" not in response.text


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _flip_last_byte(segment: str) -> str:
    raw = bytearray(_unb64(segment))
    raw[-1] ^= 0x01
    return _b64(bytes(raw))


def _forged(payload: dict[str, object]) -> str:
    return _b64(json.dumps(payload).encode())


_CURSOR_PAGE = "/products/?pagination=cursor&limit=1&sort=id&direction=ASC"


def _malformed_cursors(issued: str) -> dict[str, str]:
    payload, tag = issued.split(".")
    naive = {"b": "sqlalchemy", "k": [{"type": "datetime", "v": "2026-09-06T12:30:00"}], "id": 1}
    unknown_tag = {"b": "sqlalchemy", "k": [{"type": "money", "v": "1"}], "id": 1}
    return {
        "alphabet": "not base64!",
        "json": f"{_b64(b'{not json')}.{tag}",
        "unsigned": payload,
        "tampered-payload": f"{_flip_last_byte(payload)}.{tag}",
        "tampered-tag": f"{payload}.{_flip_last_byte(tag)}",
        "naive-datetime": f"{_forged(naive)}.{tag}",
        "wrong-tag": f"{_forged(unknown_tag)}.{tag}",
        "wrong-backend": f"{_forged({'b': 'mongo', 'k': [1], 'id': 1})}.{tag}",
        "wrong-key-count": f"{_forged({'b': 'sqlalchemy', 'k': [1, 2, 3], 'id': 1})}.{tag}",
        "other-filters": issued,
    }


def test_a_malformed_cursor_answers_400_without_echoing_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'c.sqlite'}")
    app = create_app_from_config("tests/integration/fake_repo/config/conf.modules.yaml")

    with TestClient(app, raise_server_exceptions=False) as client:
        for name in ("a", "b", "c"):
            assert client.post("/products/", json={"name": name, "price": 20.0}).status_code == 201
        issued = client.get(_CURSOR_PAGE).json()["nextCursor"]

        answers = {
            case: client.get(
                f"{_CURSOR_PAGE}&after={token}"
                + ("&price__eq=20" if case == "other-filters" else "")
            )
            for case, token in _malformed_cursors(issued).items()
        }

    statuses = {case: response.status_code for case, response in answers.items()}
    assert statuses == dict.fromkeys(answers, 400)
    for case, response in answers.items():
        detail = response.json()["detail"]
        assert detail["code"] == "unsupported_query", case
        assert _malformed_cursors(issued)[case] not in response.text, case


def test_a_cursor_issued_for_one_sort_answers_400_for_another(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 's.sqlite'}")
    app = create_app_from_config("tests/integration/fake_repo/config/conf.modules.yaml")
    by_price = "/products/?pagination=cursor&limit=1&sort=price&direction=ASC"

    with TestClient(app, raise_server_exceptions=False) as client:
        for name, price in (("a", 1.0), ("b", 2.0), ("c", 3.0)):
            assert client.post("/products/", json={"name": name, "price": price}).status_code == 201
        issued = client.get(by_price).json()["nextCursor"]

        same_sort = client.get(f"{by_price}&after={issued}")
        other_sort = client.get(f"{_CURSOR_PAGE}&after={issued}")

    assert same_sort.status_code == 200
    assert other_sort.status_code == 400
    assert other_sort.json()["detail"]["code"] == "unsupported_query"


@pytest.mark.parametrize(
    "position",
    [{"k": ["2"], "id": 2}, {"k": [2], "id": "2"}, {"k": [True], "id": 2}, {"k": [None], "id": 2}],
    ids=["str-sort-key", "str-primary-key", "bool", "null"],
)
def test_an_unsigned_cursor_holding_a_value_of_another_type_answers_400(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, position: dict[str, object]
) -> None:
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 't.sqlite'}")
    monkeypatch.setenv("LOOM__PERSISTENCE__CURSORS__SIGNING", "unsigned")
    app = create_app_from_config("tests/integration/fake_repo/config/conf.modules.yaml")

    with TestClient(app, raise_server_exceptions=False) as client:
        for name in ("a", "b", "c"):
            assert client.post("/products/", json={"name": name, "price": 1.0}).status_code == 201
        typed = client.get(
            f"{_CURSOR_PAGE}&after={_forged({'b': 'sqlalchemy', 'k': [1], 'id': 1})}"
        )
        forged = client.get(f"{_CURSOR_PAGE}&after={_forged({'b': 'sqlalchemy', **position})}")

    assert typed.status_code == 200
    assert [item["name"] for item in typed.json()["items"]] == ["b"]
    assert forged.status_code == 400
    assert forged.json()["detail"]["code"] == "unsupported_query"

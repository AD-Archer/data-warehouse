from dagster import EnvVar
from dagster_sling import SlingConnectionResource, SlingResource

from orpheus_engine.defs.sling.assets import _isolate_missing_program_connection


ENV = "ORPHEUS_TEST_MISSING_PROGRAM_DATABASE_URL"


def _conn():
    return SlingConnectionResource(
        name="TEST_PROGRAM_DB", type="postgres", connection_string=EnvVar(ENV)
    )


def test_missing_env_var_gets_invalid_placeholder(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    conn = _isolate_missing_program_connection(_conn())
    assert conn.name == "TEST_PROGRAM_DB"
    assert conn.type == "postgres"
    assert conn.connection_string == (
        "postgres://missing-env-orpheus-test-missing-program-database-url.invalid:5432/unset"
    )
    # The shared Sling resource must still resolve its config, otherwise every
    # job that includes a Sling asset fails run-config validation.
    SlingResource(connections=[conn]).process_config_and_initialize()


def test_set_env_var_is_left_alone(monkeypatch):
    monkeypatch.setenv(ENV, "postgres://user:pw@db.example.com:5432/x")
    original = _conn()
    assert _isolate_missing_program_connection(original) is original


def test_unpatched_missing_env_var_breaks_shared_resource(monkeypatch):
    """Documents the 2026-10-09 failure mode the helper guards against."""
    import pytest

    monkeypatch.delenv(ENV, raising=False)
    with pytest.raises(Exception, match=ENV):
        SlingResource(connections=[_conn()]).process_config_and_initialize()

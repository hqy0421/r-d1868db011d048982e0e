"""Tests for stable project IDs and project-key history tracking.

Jira project keys are mutable: a project can be renamed (re-keyed), and an
abandoned key can later be reused by a different project. These tests cover
the rename-safety behavior of the projects and project_versions streams.
"""

from __future__ import annotations

import json
from typing import Any

from tap_jira.streams import ProjectStream, ProjectVersionStream
from tap_jira.tap import TapJira

CONFIG = {
    "domain": "test.atlassian.net",
    "email": "test@example.com",
    "api_token": "test-token",
}

PROJECT_ID = 10001


def _make_tap(state: dict[str, Any] | None = None) -> TapJira:
    return TapJira(config=CONFIG, state=state or {})


class TestStableProjectKeys:
    """Downstream primary keys are anchored on the stable project ID."""

    def test_projects_primary_key_is_stable_id(self) -> None:
        assert ProjectStream.primary_keys == ("id",)

    def test_project_versions_primary_key_includes_stable_project_id(self) -> None:
        assert ProjectVersionStream.primary_keys == ("projectId", "id")

    def test_child_context_carries_stable_id_and_current_key(self) -> None:
        stream = ProjectStream(_make_tap())
        context = stream.get_child_context({"id": "10001", "key": "ABC"}, None)
        assert context == {"projectId": "10001", "projectKey": "ABC"}

    def test_schemas_expose_key_fields(self) -> None:
        tap = _make_tap()
        project_props = ProjectStream(tap).schema["properties"]
        assert "key" in project_props
        assert "previousKeys" in project_props

        version_props = ProjectVersionStream(tap).schema["properties"]
        assert "projectId" in version_props
        assert "projectKey" in version_props


class TestProjectKeyHistory:
    """The projects stream emits current key + historical key mapping."""

    def test_first_seen_key_has_no_previous_keys(self) -> None:
        stream = ProjectStream(_make_tap())
        record = stream.post_process({"id": "10001", "key": "ABC"}, None)
        assert record["key"] == "ABC"
        assert record["previousKeys"] == []

    def test_unchanged_key_produces_no_history(self) -> None:
        stream = ProjectStream(_make_tap())
        stream.post_process({"id": "10001", "key": "ABC"}, None)
        record = stream.post_process({"id": "10001", "key": "ABC"}, None)
        assert record["previousKeys"] == []

    def test_rename_is_detected_across_runs(self) -> None:
        tap = _make_tap()
        stream = ProjectStream(tap)
        stream.post_process({"id": "10001", "key": "OLD"}, None)

        # Simulate a later run after the project was renamed OLD -> NEW. State
        # is emitted as JSON STATE messages between runs, so round-trip it the
        # same way; this also proves the history mapping is JSON-serializable.
        tap2 = _make_tap(state=json.loads(json.dumps(tap.state)))
        stream2 = ProjectStream(tap2)
        record = stream2.post_process({"id": "10001", "key": "NEW"}, None)

        assert record["key"] == "NEW"
        assert record["previousKeys"] == ["OLD"]
        assert tap2.state["bookmarks"]["projects"]["project_key_history"] == {
            "10001": ["OLD", "NEW"],
        }

    def test_reused_key_on_new_project_does_not_inherit_history(self) -> None:
        stream = ProjectStream(_make_tap())
        stream.post_process({"id": "10001", "key": "OLD"}, None)
        stream.post_process({"id": "10001", "key": "NEW"}, None)

        # A different project (new stable ID) reuses the abandoned key.
        record = stream.post_process({"id": "10002", "key": "OLD"}, None)
        assert record["previousKeys"] == []

        # The renamed project's own history is unaffected.
        record = stream.post_process({"id": "10001", "key": "NEW"}, None)
        assert record["previousKeys"] == ["OLD"]

    def test_key_renamed_back_excludes_current_key(self) -> None:
        stream = ProjectStream(_make_tap())
        stream.post_process({"id": "10001", "key": "AAA"}, None)
        stream.post_process({"id": "10001", "key": "BBB"}, None)
        record = stream.post_process({"id": "10001", "key": "AAA"}, None)
        assert record["previousKeys"] == ["BBB"]

    def test_missing_id_or_key_is_tolerated(self) -> None:
        stream = ProjectStream(_make_tap())
        assert stream.post_process({"key": "ABC"}, None)["previousKeys"] == []
        assert stream.post_process({"id": "10001"}, None)["previousKeys"] == []


class TestProjectVersionPostProcess:
    """Version rows carry the stable project ID and the current project key."""

    def test_current_key_and_stable_id_from_context(self) -> None:
        stream = ProjectVersionStream(_make_tap())
        record = stream.post_process(
            {"id": "50001", "name": "1.0"},
            {"projectId": "10001", "projectKey": "NEW"},
        )
        assert record["projectKey"] == "NEW"
        assert record["projectId"] == PROJECT_ID

    def test_api_project_id_used_when_no_context(self) -> None:
        stream = ProjectVersionStream(_make_tap())
        record = stream.post_process(
            {"id": "50001", "name": "1.0", "projectId": PROJECT_ID},
            None,
        )
        assert record["projectId"] == PROJECT_ID
        assert "projectKey" not in record

    def test_context_project_id_overrides_payload(self) -> None:
        """Versions belong to the project whose endpoint was queried."""
        stream = ProjectVersionStream(_make_tap())
        record = stream.post_process(
            {"id": "50001", "name": "1.0", "projectId": 99999},
            {"projectId": "10001", "projectKey": "NEW"},
        )
        assert record["projectId"] == PROJECT_ID

    def test_versions_of_reused_key_projects_stay_separate(self) -> None:
        """Identical version names under a reused key yield distinct PKs."""
        stream = ProjectVersionStream(_make_tap())
        renamed_project_version = stream.post_process(
            {"id": "50001", "name": "1.0"},
            {"projectId": "10001", "projectKey": "NEW"},
        )
        reusing_project_version = stream.post_process(
            {"id": "50002", "name": "1.0"},
            {"projectId": "10002", "projectKey": "OLD"},
        )

        pk = ProjectVersionStream.primary_keys
        assert tuple(renamed_project_version[k] for k in pk) == (PROJECT_ID, "50001")
        assert tuple(reusing_project_version[k] for k in pk) == (10002, "50002")

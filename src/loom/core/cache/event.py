"""The records the cache module logs, and the fields they carry."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class CacheEvent(LogEvent):
    """What cached repositories, cached calls and the cache wiring log."""

    ENTRY_HIT = "loom.cache.entry_hit"
    ENTRY_MISSED = "loom.cache.entry_missed"
    PAYLOAD_MISMATCHED = "loom.cache.payload_mismatched"
    INDEX_ID_TYPE_MISMATCHED = "loom.cache.index_id_type_mismatched"
    LOAD_ABANDONED = "loom.cache.load_abandoned"
    POST_COMMIT_BUMP_FAILED = "loom.cache.post_commit_bump_failed"
    SCOPE_CHANGED_WRITE_SKIPPED = "loom.cache.scope_changed_write_skipped"
    REPOSITORY_NOT_CONFIGURED = "loom.cache.repository_not_configured"
    BINDING_KEPT = "loom.cache.binding_kept"
    CALL_NOT_CACHEABLE = "loom.cache.call_not_cacheable"
    CALL_NOT_CONFIGURED = "loom.cache.call_not_configured"
    CALL_KEY_UNRENDERABLE = "loom.cache.call_key_unrenderable"
    CALL_READ_FAILED = "loom.cache.call_read_failed"
    CALL_WRITE_FAILED = "loom.cache.call_write_failed"
    CALL_OVERRIDDEN_BY_SYNC = "loom.cache.call_overridden_by_sync"


class CacheKey(LogKey):
    """The fields of a cache record."""

    KEY = "loom.cache.key"
    ENTITY = "loom.cache.entity"
    FUNCTION = "loom.cache.function"
    REPOSITORY = "loom.cache.repository"
    KEPT_BINDING = "loom.cache.kept_binding"
    SLOT = "loom.cache.slot"
    RESULT_TYPE = "loom.cache.result_type"

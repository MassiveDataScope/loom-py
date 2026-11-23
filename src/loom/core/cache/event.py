"""The records the cache module logs, and the fields they carry."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class CacheEvent(LogEvent):
    """What cached repositories, cached calls and the cache wiring log."""

    ENTRY_HIT = "cache.entry_hit"
    ENTRY_MISSED = "cache.entry_missed"
    CUSTOM_PAYLOAD_MISMATCHED = "cache.custom_payload_mismatched"
    INDEX_ID_TYPE_MISMATCHED = "cache.index_id_type_mismatched"
    LOAD_ABANDONED = "cache.load_abandoned"
    POST_COMMIT_BUMP_FAILED = "cache.post_commit_bump_failed"
    SCOPE_CHANGED_WRITE_SKIPPED = "cache.scope_changed_write_skipped"
    REPOSITORY_NOT_CONFIGURED = "cache.repository_not_configured"
    CALLS_ALREADY_REGISTERED = "cache.calls_already_registered"
    DECORATOR_ALREADY_REGISTERED = "cache.decorator_already_registered"
    CALL_LOAD_ABANDONED = "cache.call_load_abandoned"
    CALL_NOT_CACHEABLE = "cache.call_not_cacheable"
    CALL_NOT_CONFIGURED = "cache.call_not_configured"
    CALL_KEY_UNRENDERABLE = "cache.call_key_unrenderable"
    CALL_READ_FAILED = "cache.call_read_failed"
    CALL_PAYLOAD_MISMATCHED = "cache.call_payload_mismatched"
    CALL_WRITE_FAILED = "cache.call_write_failed"
    CALL_OVERRIDDEN_BY_SYNC = "cache.call_overridden_by_sync"


class CacheKey(LogKey):
    """The fields of a cache record."""

    KEY = "loom.cache.key"
    ENTITY = "loom.cache.entity"
    FUNCTION = "loom.cache.function"
    TYPE = "loom.cache.type"

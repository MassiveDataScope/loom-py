"""Characterisation of the start-up of every composition path, on today's code (spec 011, T01a).

The tests record what each path binds and how each start-up refusal reaches
the caller into ``snapshots/``. Later tasks move this code into a pipeline
and compare against the recording. Set ``LOOM_UPDATE_SNAPSHOTS=1`` to
rewrite the recording from the current code instead of comparing.
"""

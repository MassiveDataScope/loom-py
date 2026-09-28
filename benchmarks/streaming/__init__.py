"""Reference streaming benchmark.

The orchestration modules (:mod:`run`, :mod:`compare`, :mod:`params`,
:mod:`load`, :mod:`metrics`) use the standard library only. Engine code
lives under :mod:`engines` and is imported only inside the child process
that runs one measurement.
"""

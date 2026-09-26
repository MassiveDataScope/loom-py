"""Reference streaming benchmark (spec 015, FR-007).

The orchestration modules (:mod:`run`, :mod:`compare`, :mod:`params`,
:mod:`load`, :mod:`metrics`) use the standard library only, so the runner can
drive any virtual environment. Engine code lives under :mod:`engines` and is
imported only inside the child process that runs one measurement.
"""

"""One island per provider: the module that imports the vendor SDK, and nothing else does.

Each module here imports its SDK at the top and publishes the same two names,
``MODEL_CLASS`` and ``build``, that ``ModelBuilder`` in :mod:`._shared`
describes. :mod:`loom.ai.engines.pydantic_ai._models` loads the island a
binding names through :func:`~loom.ai.registry.require_provider_sdk`, so a
missing SDK fails at start-up naming the extra, and no module of loom imports
a vendor SDK it may not have.
"""

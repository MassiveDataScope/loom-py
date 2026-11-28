"""One island per optional extra of the pydantic-ai engine, beside the model builders.

Each module here imports its SDK at the top and nothing else in the engine
does; the module that needs it loads the island by name through
:func:`~loom.ai.registry.require_provider_sdk` (or
:func:`loom.core.plugins.optional.import_optional` where the failure has its
own code), so a missing extra fails at start-up naming what to install.
"""

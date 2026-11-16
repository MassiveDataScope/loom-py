Repository API
==============

Contracts
---------

.. autosummary::
   :toctree: generated

   loom.core.stores.repository
   loom.core.stores.repository.query

SQLAlchemy adapter
------------------

.. autosummary::
   :toctree: generated

   loom.stores.sqlalchemy
   loom.stores.sqlalchemy.alembic
   loom.stores.sqlalchemy.rls
   loom.stores.sqlalchemy.rls.guard_manifest
   loom.stores.sqlalchemy.rls.integrity
   loom.stores.sqlalchemy.migrations
   loom.stores.sqlalchemy.migrations.operations

DynamoDB adapter
----------------

.. autosummary::
   :toctree: generated

   loom.stores.dynamodb
   loom.stores.dynamodb.backend
   loom.stores.dynamodb.repository

Mongo adapter
-------------

.. autosummary::
   :toctree: generated

   loom.stores.mongo
   loom.stores.mongo.backend
   loom.stores.mongo.repository

Cache layer
-----------

.. autosummary::
   :toctree: generated

   loom.core.cache
   loom.core.cache.repository
   loom.core.cache.gateway
   loom.core.cache.dependency
   loom.core.cache.decorators

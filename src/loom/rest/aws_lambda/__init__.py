"""Serve a loom application from AWS Lambda behind API Gateway.

Install the ``lambda`` extra. The entry point changes one line::

    handler = create_app("config.yaml", adapter=LambdaHTTPAdapter())
"""

from loom.rest.aws_lambda.adapter import LambdaHTTPAdapter, LambdaUnservedConfigError
from loom.rest.aws_lambda.handler import LambdaHTTPHandler

__all__ = ["LambdaHTTPAdapter", "LambdaHTTPHandler", "LambdaUnservedConfigError"]

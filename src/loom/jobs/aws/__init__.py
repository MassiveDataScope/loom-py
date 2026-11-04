"""AWS job backends; ``loom[sqs]`` brings boto3 to :mod:`loom.jobs.aws.sqs`."""

FIFO_SUFFIX = ".fifo"
"""How the URL and the ARN of a FIFO queue end."""


def is_fifo_queue(url_or_arn: str) -> bool:
    """Whether the queue *url_or_arn* names is a FIFO one."""
    return url_or_arn.endswith(FIFO_SUFFIX)


__all__ = ["FIFO_SUFFIX", "is_fifo_queue"]

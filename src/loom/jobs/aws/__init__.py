"""AWS job backends; ``loom[sqs]`` brings boto3 to :mod:`loom.jobs.aws.sqs`."""

FIFO_SUFFIX = ".fifo"  # ends the URL and the ARN of a FIFO queue

"""AWS Kinesis backend — checks shard-level iterator lag for a stream."""
from __future__ import annotations

import logging
from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_MAX_LAG_MS = 60_000  # 1 minute


class KinesisBackend(BaseBackend):
    """Checks the milliseconds-behind-latest metric for a Kinesis stream.

    Pipeline config extras:
        stream_name (str): Name of the Kinesis data stream.
        max_lag_ms  (int): Alert threshold in milliseconds (default 60 000).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        region = config.get("region_name", "us-east-1")
        self._client = boto3.client(
            "kinesis",
            region_name=region,
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
        )

    def check_pipeline(self, pipeline) -> PipelineResult:
        stream_name: str = pipeline.extras.get("stream_name", pipeline.name)
        max_lag_ms: int = int(pipeline.extras.get("max_lag_ms", _DEFAULT_MAX_LAG_MS))

        try:
            shards_resp = self._client.list_shards(StreamName=stream_name)
            shards = shards_resp.get("Shards", [])

            if not shards:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message=f"No shards found for stream '{stream_name}'",
                )

            max_observed_lag = 0
            for shard in shards:
                shard_id = shard["ShardId"]
                iter_resp = self._client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST",
                )
                iterator = iter_resp["ShardIterator"]
                records_resp = self._client.get_records(ShardIterator=iterator, Limit=1)
                lag = records_resp.get("MillisBehindLatest", 0)
                if lag > max_observed_lag:
                    max_observed_lag = lag

            if max_observed_lag <= max_lag_ms:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"Max lag {max_observed_lag} ms <= threshold {max_lag_ms} ms",
                )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"Max lag {max_observed_lag} ms exceeds threshold {max_lag_ms} ms",
            )

        except (BotoCoreError, ClientError) as exc:
            logger.exception("Kinesis check failed for '%s'", pipeline.name)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

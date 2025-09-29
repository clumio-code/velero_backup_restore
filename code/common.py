# Copyright 2024, Clumio, a Commvault Company.
#

"""Common methods and constants for the bulk restore lambda functions."""

from __future__ import annotations

import json
import logging
import os
from requests import adapters
from urllib3.util import Retry
import secrets
import string
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final, Protocol

import boto3
import botocore.exceptions
from clumioapi import clumioapi_client, exceptions
from clumioapi.exceptions import clumio_exception
from clumioapi.models import aws_tag_common_model
from utils import dates

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    EventsTypeDef = dict[str, Any]
    StatusAndMsgTypeDef = tuple[int, str]
    from clumioapi.models.list_aws_environments_response import (
        ListAWSEnvironmentsResponse,
    )

    class ListingCallable(Protocol):
        def __call__(self, filter: str | None, sort: str | None, start: int) -> Any: ...


DEFAULT_BASE_URL: Final = "https://us-west-2.api.clumio.com/"
ERROR_CODE: Final = 402
MAX_RETRY: Final = 5
START_TIMESTAMP_STR: Final = "start_timestamp"
STATUS_OK: Final = 200
FOLLOW_DEFAULT_INPUT: Final = "[This field will follow default input]"


# Define the retry strategy
retry_strategy = Retry(
    total=8,  # Total number of retries
    status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
    allowed_methods=[
        "HEAD",
        "GET",
        "OPTIONS",
        "PUT",
        "POST",
        "DELETE",
    ],  # Retry on these methods
    backoff_factor=2,  # A delay factor for exponential backoff.
    # Sleep for: {backoff factor} * (2 ** ({number of total retries} - 1))
    # e.g., 0s, 2s, 4s, 8s, 16s, 32s, 64s, 128s
)
retry_adapter = adapters.HTTPAdapter(max_retries=retry_strategy)


def parse_base_url(base_url: str) -> str:
    """Parse the base URL."""
    return base_url.removeprefix("https://")


def get_sort_and_ts_filter(
    direction: str | None, start_day_offset: int, end_day_offset: int
) -> tuple[str, dict[str, Any]]:
    """Get the sort and the timestamp filter."""
    end_timestamp_str = dates.get_max_n_days_ago(end_day_offset).strftime(
        dates.ISO_8601_FORMAT
    )
    start_timestamp_str = dates.get_midnight_n_days_ago(start_day_offset).strftime(
        dates.ISO_8601_FORMAT
    )

    sort = START_TIMESTAMP_STR
    if direction == "after":
        ts_filter = {
            START_TIMESTAMP_STR: {"$gt": start_timestamp_str, "$lte": end_timestamp_str}
        }
    elif direction == "before":
        sort = f"-{sort}"
        ts_filter = {START_TIMESTAMP_STR: {"$lte": end_timestamp_str}}
    else:
        ts_filter = {}
    return sort, ts_filter


def invoke_sdk_method(function: Callable, **kwargs: Any) -> Any:
    """Invoke the SDK method with retries to handle rate limiting.

    Args:
        function: The SDK method to be invoked.
        kwargs: The arguments to be passed to the SDK method.

    Returns:
        The response from the SDK method.
    """
    retry = 4
    count = 1
    for _ in range(retry):
        try:
            raw_response, parsed_response = function(**kwargs)
            if not raw_response.ok:
                logger.error(
                    f"Error invoking {function} with args {kwargs}: "
                    f"Response Code: {raw_response.status_code}, "
                    f"Reason: {raw_response.reason}, "
                    f"Content: {raw_response.content}"
                )
                raise exceptions.clumio_exception.ClumioException(
                    raw_response.reason, raw_response.content
                )
            return raw_response, parsed_response
        except clumio_exception.ClumioException as exception:
            if (
                exception.args[0].count("Too Many Requests") == 0
            ):  # Not a rate limit error
                raise exception
            count += 1
            if count > retry:
                raise exception
            # If the exception is due to rate limiting, wait and retry.
            time.sleep(count * 5)  # Exponential backoff
            continue
    raise exceptions.clumio_exception.ClumioException("Max retries exceeded", "")


from urllib.parse import urlparse, parse_qs


def paginated_iterator(list_method, **initial_kwargs):
    """
    A generic generator that iterates over all items from a paginated list method in the Clumio SDK.

    This assumes the list method returns a response object with:
    - _embedded.items: list of items
    - _links._next.href: URL for the next page (or None if last page)

    It handles pagination by parsing the 'start' and other query parameters from the next URL
    and updating the kwargs for subsequent calls.

    :param list_method: The SDK list method to call (e.g., controller.list_backup_aws_ebs_volumes)
    :param initial_kwargs: Initial keyword arguments for the list method (e.g., limit=100, filter='...')
    :yields: Individual items from the API responses across all pages.
    """
    kwargs = initial_kwargs.copy()

    while True:
        raw_response, parsed_response = invoke_sdk_method(
            function=list_method, **kwargs
        )

        # Yield each item if available
        if (
            hasattr(parsed_response, "embedded")
            and parsed_response.embedded
            and hasattr(parsed_response.embedded, "items")
            and parsed_response.embedded.items
        ):
            for item in parsed_response.embedded.items:
                yield item

        # Get next link if available
        if (
            not hasattr(parsed_response, "links")
            or not parsed_response.links
            or not hasattr(parsed_response.links, "p_next")
            or not parsed_response.links.p_next
        ):
            break

        next_href = parsed_response.links.p_next.href
        if not next_href:
            break

        # Parse query params from next href and update kwargs
        parsed = urlparse(next_href)
        query_params = parse_qs(parsed.query)
        kwargs = {k: v[0] for k, v in query_params.items()}


def get_total_list(function: Callable, api_filter: str, **kwargs: Any) -> list:
    """Wrapper to retry _get_total_list."""
    retry = 5
    count = 1
    for _ in range(retry):
        try:
            result = _get_total_list(function, api_filter, **kwargs)
            return result
        except clumio_exception.ClumioException as exception:
            count += 1
            if count > retry:
                raise exception
            # If the exception is due to rate limiting, wait and retry.
            time.sleep(retry)
            continue


def _get_total_list(function: Callable, api_filter: str, **kwargs: Any) -> list:
    """Get the list of all items.

    Args:
        function: A list API function call with pagination feature.
        api_filter: The filter applied to the list API as a parsable JSON document.
        kwargs:
         - sort: The sorting applied to the list API.
    """
    start = 1
    total_list = []
    while True:
        raw_response, parsed_response = function(
            filter=api_filter, start=start, **kwargs
        )
        # Raise error if raw response is not ok.
        if not raw_response.ok:
            raise exceptions.clumio_exception.ClumioException(
                raw_response.reason,
                f"Response Content: {raw_response.content}, apicall: {str(function)}, api_filter: {api_filter}",
            )
        if not parsed_response.total_count:
            break
        total_list.extend(parsed_response.embedded.items)
        if parsed_response.total_pages_count <= start:
            break
        start += 1
    return total_list


def get_environment_id(
    client: clumioapi_client.ClumioAPIClient,
    target_account: str | None,
    target_region: str | None,
) -> StatusAndMsgTypeDef:
    """Retrieve the environment for given target_account and target_region."""
    if not target_account:
        return ERROR_CODE, "target_account is required"

    if not target_region:
        return ERROR_CODE, "target_region is required."

    env_filter = {
        "account_native_id": {"$eq": target_account},
        "aws_region": {"$eq": target_region},
    }
    raw_response, parsed_response = invoke_sdk_method(
        function=client.aws_environments_v1.list_aws_environments,
        filter=json.dumps(env_filter),
    )
    if (
        not hasattr(parsed_response, "embedded")
        or not hasattr(parsed_response.embedded, "items")
        or not parsed_response.embedded.items
    ):
        return ERROR_CODE, "No environment found for the given account and region."
    return 200, parsed_response.embedded.items[0].p_id


def get_bearer_token() -> StatusAndMsgTypeDef:
    """Retrieve the bearer token from secret manager."""
    secret_arn = os.environ.get("CLUMIO_TOKEN_ARN")
    if not secret_arn:
        return 411, "CLUMIO_TOKEN_ARN environment variable is not set."
    secretsmanager = boto3.client("secretsmanager")
    try:
        secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(secret_value["SecretString"])
        bear = secret_dict.get("token", "")
        return STATUS_OK, bear
    except botocore.exceptions.ClientError as client_error:
        code = client_error.response["Error"]["Code"]
        return 411, f"Describe secret failed - {code}"


def filter_backup_records_by_tags(
    backup_records: list[dict],
    search_tag_key: str | None,
    search_tag_value: str | None,
    tag_field: str,
) -> list[dict]:
    """Filter the list of backup records by tags."""
    # Filter the result based on the tags.
    if not (search_tag_key and search_tag_value):
        return backup_records
    tags_filtered_backups = []
    for backup in backup_records:
        tags = {tag["key"]: tag["value"] for tag in backup["backup_record"][tag_field]}
        if tags.get(search_tag_key, None) == search_tag_value:
            tags_filtered_backups.append(backup)
    return tags_filtered_backups


def to_dict_or_none(obj: Any) -> dict | None:
    """Return dict version of an object if it exists, or None otherwise."""
    return obj.__dict__ if obj else None


def tags_from_dict(
    tags: list[dict[str, str]],
) -> list[aws_tag_common_model.AwsTagCommonModel]:
    """Convert list of tags from dict to AwsTagCommonModel."""
    tag_list = []
    for tag in tags:
        tag_list.append(
            aws_tag_common_model.AwsTagCommonModel(key=tag["key"], value=tag["value"])
        )
    return tag_list


def generate_random_string(length: int = 13) -> str:
    """Generate run token for restore."""
    return "".join(secrets.choice(string.ascii_letters) for _ in range(length))

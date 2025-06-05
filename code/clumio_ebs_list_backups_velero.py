# Copyright 2024, Clumio, a Commvault Company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lambda function to list EBS backups."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to list EBS backups."""
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    search_tag_key: str | None = events.get('search_tag_key', None)
    search_tag_value: str | None = events.get('search_tag_value', None)
    target: dict = events.get('target', {})
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 1)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)
    velero_manifest_dict = events.get("velero_manifest", None)
    source_volume_id = velero_manifest_dict.get("spec", {}).get("providerVolumeID", None)

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except (TypeError, ValueError) as e:
        error = f'invalid input: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url, raw_response=True)
    client = clumioapi_client.ClumioAPIClient(config)
    sort, api_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )

    try:
        # Get the clumio-assigned id of the volume.
        list_filter = {
            'volume_native_id': {'$eq': source_volume_id},
            'account_native_id': {'$eq': source_account},
        }
        ebs_volumes = common.get_total_list(
            function=client.aws_ebs_volumes_v1.list_aws_ebs_volumes,
            api_filter=json.dumps(list_filter),
        )
        if not ebs_volumes:
            return {'status': 401, 'msg': f'No volume found for {source_volume_id}'}

        # Get the backup record of the volume.
        api_filter['volume_id'] = {'$eq': ebs_volumes[0].p_id}
        raw_backup_records = common.get_total_list(
            function=client.backup_aws_ebs_volumes_v2.list_backup_aws_ebs_volumes,
            api_filter=json.dumps(api_filter),
            sort=sort,
        )
    except clumio_exception.ClumioException as e:
        return {'status': 401, 'msg': f'List backup error - {e}'}

    # Filter the result based on the source_account and source region.
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = {
                'volume_id': backup.volume_native_id,
                'backup_record': {
                    'source_backup_id': backup.p_id,
                    'source_volume_id': backup.volume_native_id,
                    'source_volume_tags': [tag.__dict__ for tag in backup.tags]
                    if backup.tags
                    else None,
                    'source_encrypted_flag': backup.is_encrypted,
                    'source_az': backup.aws_az,
                    'source_kms': backup.kms_key_native_id,
                    'source_expire_time': backup.expiration_timestamp,
                    'source_volume_type': backup.volume_type,
                    'source_iops': backup.iops,
                },
            }
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_volume_tags'
    )

    inputs = {
        'record': [],
        'velero_manifest': velero_manifest_dict
    }
    if not backup_records:
        return {'status': 404, "msg": f"volume {source_volume_id} not found in backup records", "inputs": inputs}
    inputs['record'] = backup_records[:1]
    return {"status": 200, "msg": "record found", "inputs": inputs}

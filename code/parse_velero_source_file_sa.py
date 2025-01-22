# Copyright 2024, Clumio Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from smart_open import open
from datetime import datetime
import json
import math
import boto3


def lambda_handler(events, context):
    target_region = events.get('target_region', None)
    velero_file_s3_uri = events.get('velero_file_s3_uri', None)
    velero_file_segment_size = events.get('velero_file_segment_size', 40)

    inputs = {'records': [],
              'day_offset': None}
    s3_client = boto3.client('s3', region_name=target_region)
    source_uri = velero_file_s3_uri
    # Try obtaining the start date if it's a scheduled backup.
    try:
        date_string = source_uri.split('-')[-2]
        date_format = '%Y%m%d%H%M%S'
        file_datetime = datetime.strptime(date_string, date_format)
        today = datetime.today()
        days_diff = (today - file_datetime).days
    except ValueError:
        # Use the manually inputted offset instead.
        days_diff = events.get('start_search_day_offset', 0)

    try:
        with open(source_uri, transport_params={"client": s3_client}) as fin:
            file_content = json.load(fin)
        if len(file_content) > 0:
            big_list = file_content
            bigger_list = []
            num_items = len(big_list)
            if velero_file_segment_size < num_items < (velero_file_segment_size * 39):
                num_sets = math.ceil(num_items / velero_file_segment_size)

                for count in range(0, num_sets):
                    start = velero_file_segment_size * count
                    stop = start + velero_file_segment_size
                    if stop > num_items:
                        stop = num_items
                    partial_list = big_list[start:stop]
                    if partial_list:
                        bigger_list.append(partial_list)
                    else:
                        break
            else:
                bigger_list = [[big_list]]
            inputs = {'records': bigger_list,
                      'day_offset': days_diff}
            return {"status": 200, "inputs": inputs, "msg": "parsed velero file"}
        else:
            inputs = {'records': file_content,
                      'day_offset': None}
            return {"status": 207, "inputs": inputs, "msg": "no records"}

    except Exception as e:
        msg = f"could not read file {e}"
        return {"status": 401, "inputs": inputs, "msg": msg}
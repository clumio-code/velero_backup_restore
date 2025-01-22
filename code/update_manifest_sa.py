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

from botocore.exceptions import ClientError
import io
import gzip
import json
import boto3


def lambda_handler(events, context):
    target_region = events.get('target_region', None)
    velero_file_s3_uri = events.get('velero_file_s3_uri',None)
    velero_file_s3_uri_test = events.get('velero_file_s3_uri_test', None)
    inputs = events.get("inputs", None)
    debug = events.get('debug', None)

    file_data = []
    failed_output = []
    if len(inputs) > 0:
        for lists in inputs:
            if len(lists) > 0:
                for item in lists:
                    output = item.get("output",{}).get("manifest",{})
                    if output:
                        file_data.append(output)
                        print(f"row {output}")
                    else:
                        print(f"malformed output {item}")
                        failed_output.append("item")
    else:
        msg =f"update_manifest: no inputs"
        return {"status": 407, "msg": msg}

    s3_client = boto3.client('s3', region_name=target_region)
    if velero_file_s3_uri_test:
        source_uri = velero_file_s3_uri_test
    else:
        source_uri = velero_file_s3_uri

    file_string_list = source_uri.split('/')
    _tmp = file_string_list.pop(0)
    _tmp = file_string_list.pop(0)
    bucket = file_string_list.pop(0)
    object_key = '/'.join(file_string_list)

    if debug > 5: print(f"parse_velero_source_file: bucket {bucket} object {object_key}")
    encoding = 'utf-8'
    default = None
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
            wrapper.write(json.dumps(file_data, ensure_ascii=False, default=default, indent=4))
    inmem.seek(0)
    try:
        s3_client.put_object(Bucket=bucket, Body=inmem, Key=object_key)
    except ClientError as e:
        error = e.response['Error']['Code']
        error_msg = f"failed to put file {error}"
        # print("in DataDump 03")
        return {"status": 402, "msg": error_msg}

    return {"status": 200, "msg": f"wrote {file_data} failed write {failed_output}"}

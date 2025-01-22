# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from botocore.exceptions import ClientError
import boto3
import time


def lambda_handler(events, context):
    target_region = events.get('target_region', None)
    source_backup_id = events.get('inputs', {}).get('source_backup_id', None)
    velero_manifest_dict = events.get('inputs', {}).get('velero_manifest', None)
    source_volume_id = velero_manifest_dict.get("spec", {}).get("providerVolumeID")

    volume_deleted = False
    outputs = {
        'manifest': velero_manifest_dict,
        'snapshot_id': None,
        'volume_deletion_status': volume_deleted
    }

    # Get the restored volume for a given source backup id.
    client_ebs = boto3.client('ec2', region_name=target_region)
    try:
        desc_volume = client_ebs.describe_volumes(
            Filters=[
                {
                    'Name': 'tag:source_backup_id',
                    'Values': [source_backup_id]
                },
            ],
        )
    except ClientError as e:
        error = e.response['Error']['Code']
        error_msg = f"Describe Volume failed - {error}"
        return {"status": 403, "msg": error_msg, "output": outputs}
    if len(desc_volume.get('Volumes', [])) == 0:
        return {"status": 404, "msg": f"no volumes found {desc_volume}", "manifest": {}, "volume_deleted": volume_deleted}
    new_volume = desc_volume.get('Volumes', [])[0].get('VolumeId', None)
    new_snapshot_id = ''

    # Create the snapeshot from the restored volume.
    for idx in range(5):
        try:
            create_snap = client_ebs.create_snapshot(
                Description='created by eks_clumio restore process',
                VolumeId=new_volume,
                TagSpecifications=[
                    {
                        'ResourceType': 'snapshot',
                        'Tags': [
                            {
                                'Key': 'original_volume',
                                'Value': source_volume_id
                            },
                            {
                                'Key': 'original_backup_id',
                                'Value': source_backup_id
                            },
                        ]
                    },
                ],
                DryRun=False
            )
            new_snapshot_id = create_snap.get('SnapshotId', None)
            break
        except ClientError as e:
            # Add backoff retry.
            if idx < 4:
                time.sleep(idx)
                continue
            error = e.response['Error']['Code']
            error_msg = f"Create snapshot failed - {error}"
            return {"status": 405, "msg": error_msg, "output": outputs, 'error': e.response['Error']}

    # Wait until snapshot creation has started before deleting volume
    time.sleep(30)
    try:
        client_ebs.delete_volume(VolumeId=new_volume)
        volume_deleted = True
    except ClientError as e:
        error = e.response['Error']['Code']
        return {"status": 405, "msg": error, "output": outputs}
    new_velero_manifest_dict = dict(velero_manifest_dict)
    new_velero_manifest_dict["status"]["providerSnapshotID"] = new_snapshot_id
    outputs = {
        'manifest': new_velero_manifest_dict,
        'snapshot_id': None,
        'volume_deletion_status': volume_deleted,
        'deleted_volume_id': new_volume,
    }
    return {"status": 200, "msg": "done", "output": outputs}
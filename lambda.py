import boto3
import json
import os
import io
from pathlib import Path

def lambda_handler(event, context):
    print("started")

    s3 = boto3.client('s3')
    
    #create path
    Path("/tmp/result").mkdir(parents=True, exist_ok=True)

    for object_key in event["s3_object_keys"]:
        file_path = f"/tmp/result/{object_key}"
        s3.download_file(event["in_bucket_name"], object_key, file_path)

    result=os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export CPATH=/mnt/cern_root/chroot/usr/include:$CPATH && \            
        export roothome=/mnt/cern_root/root_install && \
        . ${roothome}/bin/thisroot.sh && \
        chmod +x /mnt/cern_root/root_install/bin/hadd && \
        cd /tmp &&
        hadd out.root ./result/*.root
    ''')
    
    s3.upload_file(f'/tmp/out.root', event['out_bucket_name'], event['out_file_path'])

    return {
        'statusCode': 200,
        'body': json.dumps('Extracted ROOT to EFS!'),
        'result': json.dumps(result)
    }

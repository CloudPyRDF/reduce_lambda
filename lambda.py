import base64
import json
import os

import boto3

bucket = os.environ.get('bucket')


def lambda_handler(event, context):
    ssm = boto3.client('ssm')
    s3 = boto3.client('s3')

    ranges_number = int(ssm.get_parameter(Name='ranges_num')['Parameter']['Value'])
    files_in_s3 = s3.list_objects_v2(bucket)['KeyCount']
    if ranges_number != files_in_s3:
        print(f'{files_in_s3}/{ranges_number} ready')
        return {'statusCode': 400, 'body': json.dumps(f'not yet, only {files_in_s3}/{ranges_number} ready')}

    file_paths = []
    for s3_object in s3.list_objects_v2(bucket)['Contents']:
        file_path = f"/tmp/result/{s3_object['key']}"
        s3.download_file(bucket, s3_object['key'], file_path)

    if ranges_number == 1:
        s3.upload_file(file_paths[0], bucket, 'out.pickle')
        return {'statusCode': 200, 'body': json.dumps(f'Returned merged histogram to {bucket} bucket')}

    import pickle
    print("all files are in place, merging")

    pickled_reducer = base64.b64decode(ssm.get_parameter(Name='reducer')['Parameter']['Value'][2:-1])
    reducer = pickle.loads(pickled_reducer)

    def reduce_function(reducer, files):
        accumulator = reducer(files[0], files[1])
        for file in files[2:]:
            accumulator = reducer(accumulator, file)
        pickle.dump(accumulator, open('/tmp/out.pickle', 'w'))

    pickled_script = pickle.dumps(reduce_function)

    glue = f"""
import pickle
pickle.loads({pickled_script})({reducer},{file_paths})
"""

    script_file = open('/tmp/to_execute.py', "w")
    script_file.write(glue)
    script_file.close()

    result = os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export CPATH=/mnt/cern_root/chroot/usr/include:$CPATH && \            
        export roothome=/mnt/cern_root/root_install && \
        . ${roothome}/bin/thisroot.sh && \
        /mnt/cern_root/chroot/usr/bin/python3.7 /tmp/to_execute.py
    ''')
    output_bucket = ssm.get_parameter(Name='output_bucket')['Parameter']['Value']

    s3.upload_file(f'/tmp/out.pickle', output_bucket, 'out.pickle')

    # get rid of the existing files once processing is done
    s3.Bucket(output_bucket).objects.all().delete()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Returned merged histogram to {bucket} bucket'),
        'result': json.dumps(result)
    }

import base64
import json
import os

import boto3

bucket = os.environ.get('bucket')


def lambda_handler(event, context):
    ssm = boto3.client('ssm')
    s3 = boto3.client('s3')

    ranges_number = int(ssm.get_parameter(Name='ranges_num')['Parameter']['Value'])
    files_in_s3 = s3.list_objects_v2(Bucket=bucket)['KeyCount']
    if ranges_number != files_in_s3:
        print(f'{files_in_s3}/{ranges_number} ready')
        return {'statusCode': 400, 'body': json.dumps(f'not yet, only {files_in_s3}/{ranges_number} ready')}

    if not os.path.exists('/tmp/result'):
        os.makedirs('/tmp/result')
    file_paths = []
    for s3_object in s3.list_objects_v2(Bucket=bucket)['Contents']:
        print(s3_object)
        file_path = f"/tmp/result/{s3_object['Key']}"
        file_paths.append(file_path)
        s3.download_file(bucket, s3_object['Key'], file_path)

    if ranges_number == 1:
        s3.upload_file(file_paths[0], bucket, 'out.pickle')
        return {'statusCode': 200, 'body': json.dumps(f'Returned merged histogram to {bucket} bucket')}

    print("all files are in place, merging")

    pickled_reducer = base64.b64decode(ssm.get_parameter(Name='reducer')['Parameter']['Value'][2:-1])

    print(file_paths)

    glue = f"""
def reduce_function(reducer, files):
    file1=pickle.load(open(files[0], 'rb'))
    file2=pickle.load(open(files[1], 'rb'))
    accumulator = reducer(file1,file2)
    print("got this far not dying")
    
    for file in files[2:]:
        file_unpickled=pickle.load(open(file, 'rb'))
        accumulator = reducer(accumulator, file_unpickled)
    pickle.dump(accumulator, open('/tmp/out.pickle', 'wb'))

import cloudpickle as pickle
reduce_function(pickle.loads({pickled_reducer}),{file_paths})
"""

    script_file = open('/tmp/to_execute.py', "w")
    script_file.write(glue)
    script_file.close()

    result = os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export CPATH=/mnt/cern_root/chroot/usr/include:$CPATH && \
        export PYTHONPATH=/mnt/cern_root/root_install/PyRDF:/mnt/cern_root/root_install:$PYTHONPATH && \
        export roothome=/mnt/cern_root/root_install && \
        cd /mnt/cern_root/root_install/PyRDF && \
        . ${roothome}/bin/thisroot.sh && \
        /mnt/cern_root/chroot/usr/bin/python3.7 /tmp/to_execute.py
    ''')
    if not result:
        print("not result", result)
    output_bucket = ssm.get_parameter(Name='output_bucket')['Parameter']['Value']

    s3.upload_file(f'/tmp/out.pickle', output_bucket, 'out.pickle')

    # get rid of the existing files once processing is done
    s3.Bucket(output_bucket).objects.all().delete()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Returned merged histogram to {bucket} bucket'),
        'result': json.dumps(result)
    }

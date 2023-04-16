import boto3
session = boto3.session.Session(aws_access_key_id='ASIAT56BBZZ7LTHQW2GY', 
aws_secret_access_key='t2TwTUYvLUxmUVvAGV3mkBX2JDIX0V478u9/EHhG', 
aws_session_token='FwoGZXIvYXdzEAwaDDPe3rA29+IXXlkiwCLLATxz4Vb1RkIcy3Ge6WLyrEDO9ywDG2QZDwem/YH2zA2To/5Y9xeeeBmpk5M7wvwpSdVU1jVDJHZZlMDFRUEAfVkZe1D3KHElNdG4NJQtP7Ro037x28IQpZX9B/lNZ0tuEYVPUip6Ya6OO58oO5xiXqivwwcjJFZgGCDrFE4jDygJmdwjsHn02/q8OsKF5e1//opBuOX/GN3RWK2gML6YleHrAQ7R0wtt+D49WPJgbsCauIsYorcsqrQpuufbkFRA/ktRHw5IMY9QAk4UKMf+4qEGMi3jRK0EqAnaTNqv1ddsZCVg0siPC19/wAaUgJ+sCgkxMa0v6sa44/eftqKuhM8=', 
region_name='us-east-1')
ec2 = session.resource('ec2')
# create a new EC2 instance

instances = ec2.create_instances(
    ImageId='ami-007855ac798b5175e',
    MinCount=1,
    MaxCount=1,
    InstanceType='t2.micro',
    KeyName='spark_cluster'
)



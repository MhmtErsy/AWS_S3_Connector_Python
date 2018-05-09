import logging
import os
from S3.Exceptions import S3ResponseError
from os.path import join
from boto.s3 import connect_to_region
import boto.s3.connection
import datetime

try:
    conn = connect_to_region('<region>',
                                     aws_access_key_id='<access_key_id>',
                                     aws_secret_access_key='<secret_access_key>',
                                     host='<host>',
                                     #is_secure=True,               # uncomment if you are not using ssl
                                     calling_format=boto.s3.connection.OrdinaryCallingFormat(),)
except Exception, e:
    logging.error('Failed: '+str(e))




today = str(datetime.date.today())
today = today.replace(' ','-')


def DeleteDirectoryOrBucketFiles(bucket_name, directory_name=None):
    bucket = conn.get_bucket('{}'.format(bucket_name))
    if directory_name is not None:
        bucket_list_result_set = bucket.list(prefix="{}".format(directory_name))
    else:
        bucket_list_result_set = bucket.list()
    print("---- Following Files Will Be Delete ----")
    print("---- BUCKET: {} ----".format(bucket_name))
    for i in bucket_list_result_set:
        print(i.name + " is removing...")
        bucket.delete_key(i.name)


def SendFilesToBucket(bucket_name, directory_name,local_path, keyname):
    bucket = conn.get_bucket('{}'.format(bucket_name))
    key_name = keyname
    path = directory_name
    full_key_name = join(path, key_name)
    print(full_key_name)
    k = bucket.new_key(full_key_name)
    k.set_contents_from_filename(local_path+key_name)
    logfilename = "LOG-{}.txt".format(today)
    with open(logfilename, "a") as text_file:
        text_file.write("From:{} | To:{} | {}\n".format(local_path+key_name, full_key_name , str(
            datetime.datetime.today())[:-7] + "\n"))
    key_name = logfilename
    full_key_name = join(path, key_name)
    print("Log File: "+full_key_name)
    k = bucket.new_key(full_key_name)
    k.set_contents_from_filename(local_path + key_name)

def ListAllFilesFromBucket(bucket_name):
    bucket = conn.get_bucket('{}'.format(bucket_name))
    bucket_list_result_set = bucket.list()
    print("|---- BUCKET: {} ----|".format(bucket_name))
    for l in bucket_list_result_set:
        key_string = str(l.key)
        print(key_string)



#SendFilesToBucket('<BUCKET_NAME>', '<SUB_FOLDER_IN_BUCKET>', '<MY_LOCAL_PATH>' , '<FILE_FULL_NAME_WHICH_I_SEND>')
#DeleteDirectoryOrBucketFiles('<BUCKET_NAME>', '<SUB_FOLDER_IN_BUCKET>')
#ListAllFilesFromBucket('<BUCKET_NAME>')
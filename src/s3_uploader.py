# lambda packaging
# pip install --target dist -r requirements.txt
# (cp src/s3_uploader.py dist/ && cd dist && zip -1 -r -u ../s3_uploader.zip ./*)
# AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx AWS_DEFAULT_REGION=us-east-1 aws lambda update-function-code --function-name s3Copier --zip-file fileb://s3_uploader.zip

import json
import boto3
import botocore
import random
import string
import time
import asyncio
import os
import aioboto3
import concurrent

print('Loading function')


def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    # print("Random string of length", length, "is:", result_str)
    return result_str


parallelism = os.cpu_count() + 4
client_config = botocore.config.Config(
    max_pool_connections=parallelism,
)
s3 = boto3.client('s3', config=client_config)


def populate_data(bucket, prefix, num_files, str_length):
    for n in range(num_files):
        s3_key = f"{prefix}/{n:07}.txt"
        print(s3_key)
        random_string = get_random_string(str_length)
        s3.put_object(Body=random_string, Bucket=bucket, Key=s3_key)


def s3_lister(bucket, prefix, page_size=1000):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix,
                                       PaginationConfig={'PageSize': page_size, 'MaxItems': 2000})
    s3_objects = []
    for page in page_iterator:
        s3_objects.clear()
        if "Contents" in page:
            for s3_object in page["Contents"]:
                s3_objects.append(s3_object["Key"])
        yield s3_objects


def copy_object(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key):
    s3_filename = s3_key[(s3_key.index('/') + 1):]
    dest_key = f"{dest_prefix}/{s3_filename}"
    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={'Bucket': src_bucket, 'Key': s3_key})


def copy_serial(src_bucket, src_prefix, dest_bucket, dest_prefix):
    print("running in serial mode")
    for s3_objects in s3_lister(src_bucket, src_prefix):
        for s3_key in s3_objects:
            copy_object(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key)
            # print(dest_key)


def copy_parallel(src_bucket, src_prefix, dest_bucket, dest_prefix):
    print("running in parallel mode")
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for s3_objects in s3_lister(src_bucket, src_prefix):
            for s3_key in s3_objects:
                futures.append(
                    executor.submit(copy_object, src_bucket=src_bucket, src_prefix=src_prefix, dest_bucket=dest_bucket,
                                    dest_prefix=dest_prefix, s3_key=s3_key))
    for future in concurrent.futures.as_completed(futures):
        future.result()


# async def copy_object_async(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key):
async def copy_object_async(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key):
    s3_filename = s3_key[(s3_key.index('/') + 1):]
    dest_key = f"{dest_prefix}/{s3_filename}"
    async with aioboto3.client("s3") as s3_async:
        print(f"begin copying {src_bucket}/{s3_key} to {dest_bucket}/{dest_key}")
        task = await s3_async.copy_object(Bucket=dest_bucket,
                                          Key=dest_key,
                                          CopySource={'Bucket': src_bucket, 'Key': s3_key})
        print(f"done copying {src_bucket}/{s3_key} to {dest_bucket}/{dest_key}")

    return task

async def copy_async_parallel(src_bucket, src_prefix, dest_bucket, dest_prefix):
    print("running in async with threads mode")
    loop = asyncio.get_running_loop()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        for s3_objects in s3_lister(src_bucket, src_prefix, page_size=1000):
            for src_key in s3_objects:
                results.append(await loop.run_in_executor(executor, copy_object_async, src_bucket, src_prefix, dest_bucket, dest_prefix, src_key))
    print("before awaiting all results...")
    await asyncio.gather(*results)
    print("after awaiting all results...")

async def copy_async(src_bucket, src_prefix, dest_bucket, dest_prefix):
    # def copy_async(src_bucket, src_prefix, dest_bucket, dest_prefix):
    print("running in async mode")
    tasks = []
    # s3_async = aioboto3.client('s3')
    # async with aioboto3.client("s3") as s3_async:
    for s3_objects in s3_lister(src_bucket, src_prefix, page_size=1000):
        # tasks = []
        for src_key in s3_objects:
            tasks.append(asyncio.create_task(copy_object_async(src_bucket, src_key, dest_bucket, dest_prefix, src_key)))
            # tasks.append(copy_object_async(src_bucket, src_key, dest_bucket, dest_prefix, src_key, s3_async))
            # asyncio.run(copy_object_async(src_bucket, src_key, dest_bucket, dest_prefix, src_key))
    print("before awaiting all tasks...")
    await asyncio.gather(*tasks)
    print("after awaiting all tasks...")


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    bucket = event['bucket']
    print("Got bucket: ", bucket)
    # populate_data(bucket=bucket, prefix="data",num_files=1,str_length=1000)
    time.perf_counter()
    # copy_serial(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data")
    # copy_parallel(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data")
    asyncio.run(copy_async(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data"))
    # asyncio.get_event_loop().run_until_complete(copy_async(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data"))
    # copy_async(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data")
    # asyncio.run(copy_async_parallel(src_bucket=bucket, src_prefix="data", dest_bucket=bucket, dest_prefix="new_data"))
    print("time taken: ", (time.perf_counter()))
    print("cpu count: ", os.cpu_count())


if __name__ == "__main__":
    lambda_handler({'bucket': 'rndm-data'}, None)

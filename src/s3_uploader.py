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
import threading
import functools

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


def s3_lister(bucket, prefix, page_size=1000, num_items=500):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix,
                                       PaginationConfig={'PageSize': page_size, 'MaxItems': num_items})
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


def copy_serial(src_bucket, src_prefix, dest_bucket, dest_prefix, num_items):
    print("running in serial mode")
    for s3_objects in s3_lister(src_bucket, src_prefix, num_items=num_items):
        for s3_key in s3_objects:
            copy_object(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key)
            # print(dest_key)


def copy_parallel(src_bucket, src_prefix, dest_bucket, dest_prefix, num_items):
    print("running in parallel mode")
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for s3_objects in s3_lister(src_bucket, src_prefix, num_items=num_items):
            for s3_key in s3_objects:
                futures.append(
                    executor.submit(copy_object, src_bucket=src_bucket, src_prefix=src_prefix, dest_bucket=dest_bucket,
                                    dest_prefix=dest_prefix, s3_key=s3_key))
    for future in concurrent.futures.as_completed(futures):
        future.result()


async def copy_object_async(src_bucket, src_prefix, dest_bucket, dest_prefix, s3_key):
    s3_filename = s3_key[(s3_key.index('/') + 1):]
    dest_key = f"{dest_prefix}/{s3_filename}"
    async with aioboto3.client("s3") as s3_async:
        # print(f"{threading.get_ident()}: begin copying {src_bucket}/{s3_key} to {dest_bucket}/{dest_key}")
        task = await s3_async.copy_object(Bucket=dest_bucket,
                                          Key=dest_key,
                                          CopySource={'Bucket': src_bucket, 'Key': s3_key})
        # print(f"{threading.get_ident()}: done copying {src_bucket}/{s3_key} to {dest_bucket}/{dest_key}")

    return task


async def copy_async_parallel(src_bucket, src_prefix, dest_bucket, dest_prefix, num_items):
    print("running in async with threads mode")
    loop = asyncio.get_running_loop()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        for s3_objects in s3_lister(src_bucket, src_prefix, num_items=num_items):
            for src_key in s3_objects:
                results.append(
                    # loop.run_in_executor(executor, copy_object_async, src_bucket, src_prefix, dest_bucket, dest_prefix, src_key))
                    loop.run_in_executor(executor, copy_object, src_bucket, src_prefix, dest_bucket, dest_prefix,
                                         src_key))
    print(f"before awaiting {len(results)} results...")
    await asyncio.gather(*results)
    print(f"after awaiting {len(results)} results...")


# from: https://stackoverflow.com/questions/46074841/why-coroutines-cannot-be-used-with-run-in-executor
async def run_async(s3_objects, src_bucket, src_prefix, dest_bucket, dest_prefix):
    tl_loop = None
    try:
        tl_loop = asyncio.get_running_loop()
    except RuntimeError:
        print("Got runtime error, ignoring...")
    print(f"{threading.get_ident()}: got running loop: {str(tl_loop)}")
    if not tl_loop:
        tl_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(tl_loop)
        print(f"{threading.get_ident()}: made new running loop: {tl_loop}")
    tasks = []
    for s3_key in s3_objects:
        tasks.append(
            tl_loop.create_task(copy_object_async(src_bucket=src_bucket, src_prefix=src_prefix, dest_bucket=dest_bucket,
                                                  dest_prefix=dest_prefix, s3_key=s3_key)))
    await asyncio.gather(*tasks)

def run_sync(s3_objects, src_bucket, src_prefix, dest_bucket, dest_prefix):
    asyncio.run(run_async(s3_objects, src_bucket, src_prefix, dest_bucket, dest_prefix))

async def copy_async_parallel2(src_bucket, src_prefix, dest_bucket, dest_prefix, num_items):
    print("running in async with threads mode v2")
    loop = asyncio.get_running_loop()
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        for s3_objects in s3_lister(src_bucket, src_prefix, page_size=50, num_items=num_items):
            # for src_key in s3_objects:
            futures.append(
                loop.run_in_executor(executor, run_sync, s3_objects, src_bucket, src_prefix, dest_bucket, dest_prefix))
    print(f"before awaiting {len(futures)} futures...")
    await asyncio.gather(*futures)
    print(f"after awaiting {len(futures)} futures...")


async def copy_async(src_bucket, src_prefix, dest_bucket, dest_prefix, num_items):
    print("running in async mode")
    tasks = []
    for s3_objects in s3_lister(src_bucket, src_prefix, num_items=num_items):
        for src_key in s3_objects:
            tasks.append(asyncio.create_task(copy_object_async(src_bucket, src_key, dest_bucket, dest_prefix, src_key)))
    print(f"before awaiting {len(tasks)} tasks...")
    await asyncio.gather(*tasks)
    print(f"after awaiting {len(tasks)} tasks...")


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("cpu count: ", os.cpu_count())
    bucket = event['bucket']
    mode = event['mode']
    print("Got bucket: ", bucket)
    print(f"running in {mode} mode")
    num_items = event['num_items']
    print(f"processing {num_items} items")
    src_prefix = "data"
    dest_prefix = "new_data"
    # populate_data(bucket=bucket, prefix="data",num_files=1,str_length=1000)
    start = time.time()
    if mode == "serial":
        copy_serial(src_bucket=bucket, src_prefix=src_prefix, dest_bucket=bucket, dest_prefix=dest_prefix,
                    num_items=num_items)
    elif mode == "parallel":
        copy_parallel(src_bucket=bucket, src_prefix=src_prefix, dest_bucket=bucket, dest_prefix=dest_prefix,
                      num_items=num_items)
    elif mode == "async":
        asyncio.run(copy_async(src_bucket=bucket, src_prefix=src_prefix, dest_bucket=bucket, dest_prefix=dest_prefix,
                               num_items=num_items))
    elif mode == "async-parallel":
        asyncio.run(
            copy_async_parallel(src_bucket=bucket, src_prefix=src_prefix, dest_bucket=bucket, dest_prefix=dest_prefix,
                                num_items=num_items))
    elif mode == "async-parallel2":
        asyncio.run(
            copy_async_parallel2(src_bucket=bucket, src_prefix=src_prefix, dest_bucket=bucket, dest_prefix=dest_prefix,
                                 num_items=num_items))
    print("time taken: ", (time.time() - start))


if __name__ == "__main__":
    lambda_handler({'bucket': 'rndm-data', 'mode': 'async-parallel2', 'num_items': 2000}, None)

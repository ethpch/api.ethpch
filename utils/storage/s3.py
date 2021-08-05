"""
Depend on aiobotocore.
https://github.com/aio-libs/aiobotocore
"""
import asyncio
from logging import getLogger
from pathlib import Path
from functools import partial
from math import ceil
from typing import Literal, Tuple, Union
from aiobotocore.session import get_session
import aiofiles
from botocore.exceptions import ClientError
from utils.config import s3

FILE_CHUNK_SIZE = 5 * 1024 * 1024  # minimum size: 5MB

ENDPOINT_URL = s3.endpoint_url
AWS_ACCESS_KEY_ID = s3.aws_access_key_id
AWS_SECRET_ACCESS_KEY = s3.aws_secret_access_key
API_BUCKET = s3.api_bucket

_session = get_session()
_create_client = partial(
    _session.create_client,
    service_name='s3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

_part_info = {'Parts': []}

logger = getLogger('api_ethpch')


# s3cmd-style apis
async def mb(bucket: str) -> bool:
    """ Make bucket """
    async with _create_client() as client:
        try:
            await client.create_bucket(Bucket=bucket)
            logger.info(f'Make bucket "{bucket}".')
        except ClientError:
            return False
    return True


async def rb(bucket: str) -> bool:
    """ Remove bucket """
    async with _create_client() as client:
        try:
            await client.delete_bucket(Bucket=bucket)
            logger.info(f'Remove bucket "{bucket}".')
        except ClientError:
            return False
    return True


async def ls(path: str = None) -> Tuple[str]:
    """ List objects or buckets """
    async with _create_client() as client:
        if path is None:
            resp = await client.list_buckets()
            logger.info('List all buckets.')
            return tuple([item['Name'] for item in resp['Buckets']])
        else:
            _ = path.strip('/').split('/')
            kws = {'Bucket': _[0]}
            if len(_) > 1:
                kws.update({'Prefix': '/'.join(_[1:])})
            try:
                resp = await client.list_objects(**kws)
                logger.info(f'List objects in "{path}".')
                return tuple([
                    f'/{_[0]}/' + item['Key'] for item in resp['Contents']
                ]) if 'Contents' in resp.keys() else ()
            except ClientError:
                return ()


async def la() -> Tuple[str]:
    """ List all objects in all buckets """
    li = []
    async with _create_client() as client:
        for bucket in (await client.list_buckets())['Buckets']:
            _ = await client.list_objects(Bucket=bucket['Name'])
            for item in _['Contents']:
                if item['Key'].endswith('/') is False:
                    li.append(bucket['Name'] + '/' + item['Key'])
    logger.info('List all objects in all buckets.')
    return tuple(li)


async def put(src: Union[str, bytes],
              dst: str,
              replace: bool = True,
              public_read: bool = False) -> bool:
    """ Put file into bucket """
    if replace is False and await has(dst) is True:
        return True
    _ = dst.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    # about multipart upload, see
    # https://skonik.me/uploading-large-file-to-s3-using-aiobotocore/
    if isinstance(src, str) is True:
        file = Path(src).resolve()
        if dst.endswith('/'):
            key = key + '/' + file.name
        try:
            async with _create_client() as client, \
                    aiofiles.open(file, 'rb') as f:
                if file.stat().st_size <= FILE_CHUNK_SIZE:  # 5MB small file
                    await client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=await f.read(),
                        ACL='public-read' if public_read else 'private')
                    logger.info(f'Put object "{src}" to "{dst}".')
                    return True
                else:  # large file upload, using multipart
                    src_size = file.stat().st_size
                    chunks_count = ceil(src_size / FILE_CHUNK_SIZE)
                    create_mp_upload_resp = \
                        await client.create_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            ACL='public-read' if public_read else 'private')
                    upload_id = create_mp_upload_resp['UploadId']
                    tasks = []

                    async def upload_chunk(client, file, upload_id,
                                           chunk_number, src_size, bucket,
                                           key):
                        offset = chunk_number * FILE_CHUNK_SIZE
                        remaining_bytes = src_size - offset
                        bytes_to_read = min((FILE_CHUNK_SIZE, remaining_bytes))
                        part_number = chunk_number + 1

                        file.seek(offset)
                        data = await file.read(bytes_to_read)
                        resp = await client.upload_part(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                            Body=data,
                            PartNumber=part_number,
                        )
                        global _part_info
                        _part_info['Parts'].append({
                            'PartNumber': part_number,
                            'ETag': resp['ETag']
                        })

                    for chunk_number in range(chunks_count):
                        tasks.append(
                            upload_chunk(
                                client=client,
                                file=f,
                                upload_id=upload_id,
                                chunk_number=chunk_number,
                                src_size=src_size,
                                bucket=bucket,
                                key=key,
                            ))
                    await asyncio.gather(*tasks)

                    list_parts_resp = await client.list_parts(
                        Bucket=bucket, Key=key, UploadId=upload_id)

                    part_list = sorted(_part_info['Parts'],
                                       key=lambda k: k['PartNumber'])
                    _part_info['Parts'] = part_list

                    if len(list_parts_resp['Parts']) == chunks_count:
                        await client.complete_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                            MultipartUpload=_part_info)
                        logger.info(f'Put object "{src}" to "{dst}".')
                        return True
                    else:
                        await client.abort_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                        )
                        return False
        except (ClientError, FileNotFoundError):
            return False
    elif isinstance(src, bytes) is True:
        if dst.endswith('/'):
            key = key + '/'
        try:
            async with _create_client() as client:
                if (src_size := len(src)) <= FILE_CHUNK_SIZE:
                    await client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=src,
                        ACL='public-read' if public_read else 'private')
                    logger.info(f'Put {src_size} bytes to "{dst}".')
                    return True
                else:
                    chunks_count = ceil(src_size / FILE_CHUNK_SIZE)

                    create_mp_upload_resp = \
                        await client.create_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            ACL='public-read' if public_read else 'private')
                    upload_id = create_mp_upload_resp['UploadId']
                    tasks = []

                    async def upload_chunk(client, file, upload_id,
                                           chunk_number, bucket, key):
                        offset = chunk_number * FILE_CHUNK_SIZE
                        part_number = chunk_number + 1

                        data = file[offset:offset + FILE_CHUNK_SIZE]
                        resp = await client.upload_part(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                            Body=data,
                            PartNumber=part_number,
                        )
                        global _part_info
                        _part_info['Parts'].append({
                            'PartNumber': part_number,
                            'ETag': resp['ETag']
                        })

                    for chunk_number in range(chunks_count):
                        tasks.append(
                            upload_chunk(
                                client=client,
                                file=src,
                                upload_id=upload_id,
                                chunk_number=chunk_number,
                                bucket=bucket,
                                key=key,
                            ))
                    await asyncio.gather(*tasks)

                    list_parts_resp = await client.list_parts(
                        Bucket=bucket, Key=key, UploadId=upload_id)

                    part_list = sorted(_part_info['Parts'],
                                       key=lambda k: k['PartNumber'])
                    _part_info['Parts'] = part_list

                    if len(list_parts_resp['Parts']) == chunks_count:
                        await client.complete_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                            MultipartUpload=_part_info)
                        logger.info(f'Put {src_size} bytes to "{dst}".')
                        return True
                    else:
                        await client.abort_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                        )
                        return False
        except ClientError:
            return False
        finally:
            del src  # release memory


async def get(src: str, dst: str = None) -> Union[bytes, bool]:
    """ Get file from bucket """
    _ = src.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    async with _create_client() as client:
        try:
            resp = await client.get_object(Bucket=bucket, Key=key)
        except ClientError:
            return False
        async with resp['Body'] as stream:
            if dst is not None:
                async with aiofiles.open(dst, 'w+b') as f:
                    async for chunk in stream.iter_chunked(FILE_CHUNK_SIZE):
                        await f.write(chunk)
                    logger.info(f'Get object "{src}" to "{dst}".')
                    return True
            else:
                logger.info(f'Get object "{src}" as return.')
                return await stream.read()


async def rm(path: str):
    """ Delete file from bucket """
    _ = path.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    if path.endswith('/'):
        key += '/'
    async with _create_client() as client:
        try:
            await client.delete_object(Bucket=bucket, Key=key)
            logger.info(f'Delete file "{path}" from bucket.')
        except ClientError:
            pass


async def du() -> Tuple[Tuple[int, int, str]]:
    """ Disk usage by buckets """
    async with _create_client() as client:
        buckets = [
            item['Name'] for item in (await client.list_buckets())['Buckets']
        ]
        ret = []
        for bucket in buckets:
            try:
                objs = (await client.list_objects(Bucket=bucket))['Contents']
                size = sum([obj['Size'] for obj in objs])
                count = len(objs)
                ret.append((size, count, bucket))
            except KeyError:
                ret.append((0, 0, bucket))
        logger.info('Show disk usage by buckets.')
        return tuple(ret)


async def cp(src: str, dst: str) -> bool:
    """ Copy object """
    _ = dst.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    try:
        async with _create_client() as client:
            await client.copy_object(Bucket=bucket, Key=key, CopySource=src)
            logger.info(f'Copy object "{src}" to "{dst}".')
            return True
    except ClientError:
        return False


async def mv(src: str, dst: str) -> bool:
    """ Move object """
    if await cp(src, dst):
        await rm(src)
        logger.info(f'Move object "{src}" to "{dst}".')


# other apis
async def _modify_bucket_acl(
    bucket: str,
    acl: Literal['private', 'public-read', 'public-read-write',
                 'aws-exec-read', 'authenticated-read', 'bucket-owner-read',
                 'bucket-owner-full-control', 'log-delivery-write']
) -> bool:
    # about ACL, see
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html
    try:
        async with _create_client() as client:
            await client.put_bucket_acl(Bucket=bucket, ACL=acl)
            logger.info(f'Modify ACL of bucket "{bucket}" as "{acl}".')
            return True
    except ClientError:
        return False


async def _modify_object_acl(
    path: str,
    acl: Literal['private', 'public-read', 'public-read-write',
                 'aws-exec-read', 'authenticated-read', 'bucket-owner-read',
                 'bucket-owner-full-control', 'log-delivery-write']
) -> bool:
    # about ACL, see
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html
    _ = path.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    try:
        async with _create_client() as client:
            await client.put_object_acl(Bucket=bucket, Key=key, ACL=acl)
            logger.info(f'Modify ACL of object "{path}" as "{acl}".')
            return True
    except ClientError:
        return False


async def _generate_temporary_url(path: str, expiration: int = 3600) -> str:
    _ = path.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    async with _create_client() as client:
        try:
            url = await client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=expiration,
            )
            logger.info('Generate presigned url expiring in '
                        f'{expiration} seconds for object "{path}".')
            return url
        except ClientError:
            return ''


async def url(path: str, expiration: int = None) -> str:
    """ Get public url of designated object """
    _ = path.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    async with _create_client() as client:
        try:
            acl = await client.get_object_acl(Bucket=bucket, Key=key)
        except ClientError:
            return ''
        if expiration or 'http://acs.amazonaws.com/groups/global/AllUsers' \
                not in str(acl['Grants']):
            return await _generate_temporary_url(path, expiration or 3600)
        else:
            logger.info(f'Get permanent url for object "{path}".')
            return ENDPOINT_URL.rstrip('/') + '/' + path.lstrip('/')


async def has(path: str) -> bool:
    """ Test whether object exists in bucket """
    _ = path.strip('/').split('/')
    bucket = _[0]
    key = '/'.join(_[1:])
    async with _create_client() as client:
        try:
            await client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False


__all__ = ('mb', 'rb', 'ls', 'la', 'put', 'get', 'has', 'rm', 'du', 'cp', 'mv',
           'url')

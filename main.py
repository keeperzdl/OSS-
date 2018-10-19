#!/usr/bin/env.python
# -*- coding: utf-8 -*-

import os
import oss2
from itertools import islice
import redis
import time
import multiprocessing
from system_conf import *

print "test"

class Database:
    def __init__(self):
        self.host = REDIS_HOSTNAME
        self.port = REDIS_PORT
        self.db = REDIS_DB
        self.password = REDIS_PASSWORD

class PTD_LOG_DOWNLOAD:
    def __init__(self):
        self.handle = None
        return

    def connect_oss(self, access_key_id, access_key_secret, endpoint, bucket_name):
        '''
        初始化连接OSS
        '''
        try:
            auth = oss2.Auth(access_key_id, access_key_secret)
            bucket = oss2.Bucket(auth, endpoint, bucket_name,connect_timeout=30)
            return bucket
        except Exception, e:
            return False

    def __download_file__(self,list,save_path):
        '''
        下载文件
        '''
        pool = redis.ConnectionPool(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
        r = redis.StrictRedis(connection_pool=pool)
        handle = PTD_LOG_DOWNLOAD()
        bucket = handle.connect_oss(OSS_ACCESSKEYID, OSS_ACCESSKEYSECRET, OSS_ENDPOINT, OSS_BUCKET_NAME)
        try:
            # for path in list:
            path = list[2]  #oss文件全路径名
            full_path = save_path+path         #本地保存文件全路径名
            local_path = full_path.split(full_path.split('/')[-1])[0]   #F:/zdl/ptd_2NHF5K2/Malicious/20171220/
            if r.exists(path):
                # continue
                pass
            else:
                if os.path.exists(local_path) == False:  # 判断文件夹是否存在
                    os.makedirs(local_path)
                bucket.get_object_to_file(path,full_path)
                r.set(path, full_path)
        except Exception, exception:
            print exception
            time.sleep(3)
            handle = PTD_LOG_DOWNLOAD()
            handle.__download_file__(list,save_path)


    def put_file(self, local_path, ali_path):
        '''
        上传文件
        '''
        handle = PTD_LOG_DOWNLOAD()
        bucket = handle.connect_oss(OSS_ACCESSKEYID, OSS_ACCESSKEYSECRET, OSS_ENDPOINT, OSS_BUCKET_NAME)
        for dirpath, dirnames, filenames in os.walk(local_path):
            for file in filenames:
                fullpath = os.path.join(dirpath, file)
                ali_filename = fullpath.split('\\')[-1]#从windows上传
                # ali_filename = fullpath.split('/')[-1]#从linux上传
                bucket.put_object_from_file(ali_path + ali_filename, fullpath)

    def delete(self):
        '''
        删除redis中key(path)对应的object
        '''
        pool = redis.ConnectionPool(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
        r = redis.StrictRedis(connection_pool=pool)
        handle = PTD_LOG_DOWNLOAD()
        bucket = handle.connect_oss(OSS_ACCESSKEYID, OSS_ACCESSKEYSECRET, OSS_ENDPOINT, OSS_BUCKET_NAME)
        list_delect = r.keys()
        for i in list_delect:
            bucket.delete_object(i)


    def eachFile(self):
        '''
        返回所有文件的全路径名(list)
        '''
        handle = PTD_LOG_DOWNLOAD()
        bucket = handle.connect_oss(OSS_ACCESSKEYID,OSS_ACCESSKEYSECRET,OSS_ENDPOINT,OSS_BUCKET_NAME)
        if handle != False:
            list = []
            for b in oss2.ObjectIterator(bucket):
                list.append(b.key)
            return list
if __name__ == '__main__':
    obj = PTD_LOG_DOWNLOAD()
    #上传本地路径，oss路径
    # obj.put_file('F:\zdl\ptd_2NHF5K2\Malicious\\', 'ptd/MaliciousFile/')
    #四个进程下载
    handle = obj.eachFile()
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    n = 1
    for path in handle:
        if n % 4 == 1:
            list1.append(path)
        if n % 4 == 2:
            list2.append(path)
        if n % 4 == 3:
            list3.append(path)
        if n % 4 == 0:
            list4.append(path)
        n += 1
    p1 = multiprocessing.Process(target=obj.__download_file__, args=(list1,SAVE_PATH))
    p2 = multiprocessing.Process(target=obj.__download_file__, args=(list2,SAVE_PATH))
    p3 = multiprocessing.Process(target=obj.__download_file__, args=(list3,SAVE_PATH))
    p4 = multiprocessing.Process(target=obj.__download_file__, args=(list4,SAVE_PATH))

    p1.start()
    p2.start()
    p3.start()
    p4.start()

    # pool = multiprocessing.Pool(processes=3)
    # pool.apply_async(obj.__download_file__,(handle,SAVE_PATH,))
    # pool.close()
    # pool.join()
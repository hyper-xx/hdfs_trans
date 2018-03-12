# -*- coding: utf-8 -*-
__author__ = 'Xu'

import threading
import time
import queue
import json, mimetypes
import os
import time
import hdfs
from hdfs import InsecureClient
import logging
import configparser
from collections import defaultdict

queuepipe = queue.Queue(300)
logging.basicConfig(filename=os.path.join(os.getcwd(), 'getduiba.log'), level=logging.INFO, filemode='a',
                    format='%(asctime)s - %(levelname)s: %(message)s')

cfg=configparser.ConfigParser()
cfg.read('config.conf')
cfg.sections()
hdfs_mvpath=cfg.get('path','hdfs_mvpath')
tmp_path=cfg.get('path','tmp_path')
oldhdfs_url=cfg.get('oldhdfs','url')
oldhdfs_root=cfg.get('oldhdfs','root')
newhdfs_url=cfg.get('newhdfs','url')
newhdfs_root=cfg.get('newhdfs','root')


oldhdfs = hdfs.Client(oldhdfs_url, root=oldhdfs_root, timeout=100, session=False)
# newhdfs = hdfs.Client(newhdfs_url, root=newhdfs_root, timeout=100, session=False)

#oldhdfs = InsecureClient(oldhdfs_url, user="hdfs")
newhdfs = InsecureClient(newhdfs_url, user="hdfs")

L=threading.Lock()


class Producer(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        for root, path, files in oldhdfs.walk(hdfs_mvpath):
            for file in files:
                full_path = os.path.join(root, file)
                data=[full_path,root,file]
                queuepipe.put(data, block=True, timeout=None)
                #logging.info(full_path)



class Consumer(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.name=name


    def callback(self,path,nbytes,history=defaultdict(list)):
        history[path].append(nbytes)
        return history

    def cb(self,path, nbytes, chunk_lengths=[]):
        chunk_lengths.append(nbytes)
        return chunk_lengths


    def get_oldhdfs_file(self,hdfs_url,hdfs_dir):
        isExits=os.path.exists('./down'+hdfs_dir)
        if not isExits:
            os.makedirs('./down'+hdfs_dir)
            oldhdfs.download(hdfs_url,'./down'+hdfs_url,overwrite=True,n_threads=16)
        else:
            oldhdfs.download(hdfs_url, './down' + hdfs_url, overwrite=True, n_threads=16)


    def put_newhdfs_file(self,hdfs_dir,tmpfile):
        newhdfs.upload(hdfs_dir,tmpfile,overwrite=True,n_threads=16,chunk_size=64000)

    ###format {'fileId': 244471141, 'permission': '644', 'replication': 3, 'blockSize': 134217728, 'group': 'supergroup', 'type': 'FILE', 'storagePolicy': 0, 'accessTime': 1520218141205, 'modificationTime': 1520014998148, 'length': 3515361, 'childrenNum': 0, 'owner': 'xgf', 'pathSuffix': ''}
    def get_oldfile_status(self,hdfs_url):
        data=oldhdfs.status(hdfs_url,strict=False)
        return data

    def get_newfile_status(self,hdfs_url):
        data=newhdfs.status(hdfs_url,strict=False)
        return data

    def del_tmpfile(self,file):
        downfile = os.path.join(os.path.abspath('.'), file)
        if os.path.exists(downfile):
            os.remove(downfile)



    def run(self):
        while 1:

            hdfs_url,hdfs_dir,file=queuepipe.get()

            #size=Consumer.get_oldfile_status(self,hdfs_url)['length']
            #print(hdfs_url,hdfs_dir,file)
            if Consumer.get_newfile_status(self,hdfs_url) == None:
                Consumer.get_oldhdfs_file(self,hdfs_url,hdfs_dir)
                tmp_file='./down'+hdfs_url
                tmp_size = os.path.getsize(tmp_file)
                #print(hdfs_dir+"/",tmp_file,tmp_size)
                if tmp_size == Consumer.get_oldfile_status(self,hdfs_url)['length']:
                    Consumer.put_newhdfs_file(self,hdfs_url,tmp_file)
                    Consumer.del_tmpfile(self,tmp_file)
                    logging.info('|'+hdfs_url+'|'+'file upload success.')
                else:
                    logging.error('|'+hdfs_url+'|'+'file size error.')
            else:
                logging.warning('|'+hdfs_url+'|'+'new hdfs exitst.')



def product():
    t1 = Producer('pr')
    t1.start()
    c1=Consumer('co')
    c1.start()


    ###daemon thread
    def check_thread(sleeptimes=180,initThreadsName=[]):
        for i in range(0,10080):
            nowThreadsName=[]
            now=threading.enumerate()
            for i in now:
                nowThreadsName.append(i.getName())

            for co in initThreadsName:
                if  co in nowThreadsName:
                    pass
                else:
                    logging.warning ('|'+co+'|'+'stopped，now restart thread.')
                    t=Consumer(str(co))
                    t.start()
            time.sleep(sleeptimes)


    for i in range(8):
        t = Consumer(str(i))
        t.start()


    initThreadsName=[]
    init=threading.enumerate()
    for i in init:
        initThreadsName.append(i.getName())
    check=threading.Thread(check_thread(180,initThreadsName))
    check.start()


if __name__ == "__main__":
    product()

# -*- coding: utf-8 -*-
__author__ = 'Xu'

import threading
import time
import queue
import json, mimetypes
import requests
import os
import time,datetime
import hdfs
from hdfs import InsecureClient
import logging
import configparser
from collections import defaultdict

queuepipe = queue.Queue(300)
logging.basicConfig(filename=os.path.join(os.getcwd(), 'rmtmp.log'), level=logging.INFO, filemode='a',
                    format='%(asctime)s - %(levelname)s: %(message)s')

cfg=configparser.ConfigParser()
cfg.read('config.conf')
cfg.sections()
hdfs_mvpath=cfg.get('path','hdfs_mvpath')
newhdfs_url=cfg.get('newhdfs','url')
oldhdfs_url=cfg.get('oldhdfs','url')
oldhdfs_root=cfg.get('oldhdfs','root')
dingrobot=cfg.get('alert','dingrobot')
notice=cfg.get('alert','notice')

newhdfs = InsecureClient(newhdfs_url, user="hdfs")
oldhdfs = hdfs.Client(oldhdfs_url, root=oldhdfs_root, timeout=100, session=False)

class Producer(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        for root, path, files in newhdfs.walk(hdfs_mvpath):
            for file in files:
                full_path = os.path.join(root, file)
                data=[full_path,root,file]
                if '2018-03-19' not in data[0]:
                    queuepipe.put(data, block=True, timeout=None)


class Consumer(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.name=name

    def get_oldfile_status(self,hdfs_url):
        data=oldhdfs.status(hdfs_url,strict=False)
        return data

    def get_newfile_status(self,hdfs_url):
        data=newhdfs.status(hdfs_url,strict=False)
        return data

    def ding_alert(self,message):
        dingurl=dingrobot
        atmobile=list(map(str, notice))
        data={
            "msgtype":"text",
            "text":{
                "content":message
            },
            "at":{
                "atMobiles":atmobile,
            }
        }
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data=json.dumps(data)
        requests.post(dingurl,headers=headers,data=data)

    def run(self):
        while 1:
            hdfs_url, hdfs_dir, file = queuepipe.get()
            if Consumer.get_oldfile_status(self,hdfs_url) == None:
                logging.error('|'+hdfs_url+'|'+'file data not unique.')
                delstatus=newhdfs.delete(hdfs_url)
                if delstatus == True:
                    logging.warning('|'+hdfs_url+'|'+'duplicate file deleted.')
                else:
                    logging.error('|'+hdfs_url+'|'+'duplicate file delete fail.')
                    n = str(datetime.datetime.now())
                    Consumer.ding_alert(self, n + '|新集群重复数据清除失败')


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
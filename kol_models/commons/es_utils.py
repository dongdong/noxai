#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: cl
@time: 2020/8/3 19:51
"""
import json
import urllib3
urllib3.disable_warnings()
import logging
import os, sys
#reload(sys)
import importlib
importlib.reload(sys)
#sys.setdefaultencoding("utf-8")
import re
import redis
import requests
import traceback
pool_spider = redis.ConnectionPool(host='10.100.1.85', password='dhmi5xMPjjfidUx4', port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)
import time
import datetime
from dateutil.relativedelta import relativedelta
urllib3.disable_warnings()
from collections import Counter, defaultdict
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch(['http://10.100.1.176:9200'], timeout=30)
es_video = es_post = Elasticsearch(['http://10.100.1.77:9200'], timeout=30)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

#channel_index = 'kol_v2'
#channel_index = 'kol_v9'
channel_index = 'kol_v8'


def _get_video_contents(video_id, source):
    data = {
        'video_id': video_id,
        'title': source['title'],
        'category_id': source['category'],
        'keywords': source.get('keywords', []),
        'source': 'es',
    }
    return data
 

def get_video_contents(video_id):
    data = None
    try:
        source = es_video.get_source(index='kol_video', id=video_id)
        data = _get_video_contents(video_id, source)
    except:
        logging.warn('fail to get video contents. video id: ' + video_id)
    return data


def get_channel_contents(channel_id):
    data = es.get_source(index=channel_index, id=channel_id)
    data['channel_id'] = channel_id
    return data


def get_video_description(video_id):
    try:
        source = es_video.get_source(index='kol_video_description', id=video_id)
        description = source['description']
    except:
        description = ''
    return description


def get_video_description_batch(video_id_list):
    ret = {video_id:'' for video_id in video_id_list}
    try:
        param_dict = {
            'ids': video_id_list,
        }
        json_obj = es_video.mget(index='kol_video_description', body=param_dict)
        #print(results)
        docs = json_obj.get('docs', [])
        for doc in docs:
            video_id = doc['_id']
            if '_source' in doc:
                desc = doc['_source'].get('description', '')
                ret[video_id] = desc
    except:
        pass
    return ret


def get_channel_video_list(channel_id, size, pub_date_threshold=None):
    video_list = []
    try:
        param_dic = {
            "size": size,
            "query": {"bool": {"must": [
                {"term": {"channel_id": {"value": channel_id}}},
            ]}},
            "_source": [
                'title', 'category', 'language', 'description', 'keywords',
                'pub_date', 'pub_time',
            ],
            "sort":[{
               "pub_date": {"order": "desc"},
            }],
        }
        if pub_date_threshold:
            pub_date_condition = {"range": {"pub_date": {"gt": pub_date_threshold}}}
            param_dic['query']['bool']['must'].append(pub_date_condition)
        #print(param_dic)
        json_obj = es_video.search(index='kol_video', body=param_dic, timeout='1m')
        hits = json_obj['hits']['hits']
        for hit in hits:
            #video_info = hit['_source'].copy()
            #video_info['id'] = hit['_id']
            #video_list.append(video_info)
            #print(video_info)
            video_id = hit['_id']
            source = hit['_source']
            video_contents = _get_video_contents(video_id, source)
            video_list.append(video_contents)
    except:
        logging.error(traceback.format_exc())
    return video_list


def get_channel_list_by_category_language(category, language, size):
    try:
        param_dic = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"languages": {"value": language}}},
                        {"term": {"category_from_mysql": {"value": category}}},
                        {"range": {"total_videos": {"gte": 20}}},
                        {"range": {"is_delete": {"lt": 2}}},
                        {"range": {"latest_video_recent30.pub_date": {"gte": '2020-01-01'}}},
                    ]
                }
            },
            "sort":[
                {"noxStarLevel": {"order": "desc"}},
                {"total_views": {"order": "desc"}},
                {"avg_engagement_rate": {"order": "desc"}},
                {"latest_three_pub_date": {"order": "desc"}},
            ]
        }
        json_obj = es.search(index=channel_index, body=param_dic, timeout='1m')
        hits = json_obj['hits']['hits']
        channel_list = []
        for hit in hits:
            channel_info = hit['_source'].copy()
            channel_info['id'] = hit['_id']
            channel_list.append(channel_info)
        return channel_list
    except:
        logging.error(traceback.format_exc())


def write_tag_info(channel_id, tag_info_list, all_tag_name_list):
    succ = False
    es_dic = {
        "tag_detail": tag_info_list,
        "tag_list": all_tag_name_list,
    }
    try:
        #es.update(index=channel_index, doc_type='doc_profiles', id=channel_id, 
        es.update(index=channel_index, id=channel_id, body={"doc": es_dic}, 
                retry_on_conflict=3)
        succ = True
    except:
        logging.error(traceback.format_exc())
    return succ


def update_channel_contents(channel_id, data):
    param = {"retry_on_conflict": 5}
    es.update(index=channel_index, params=param, id=channel_id, 
            body={"doc": data})



def test_video_contents():
    video_id_list = [
        "YWn9xIGbZfw", "50Smb_N8V0A", "4llgRFENDs8", "Xiv7TnvqRWw", "EaL4O17cx0k", "Bj6y0tw436c", "WF6cf5sSWwI",
    ]
    i = 0
    for video_id in video_id_list:
        #video_id = '4WsFwKn-ufM'
        video_contents = get_video_contents(video_id)
        print(i)
        print(video_contents)
        i += 1


def test_channel_contents():
    #channel_id = 'UCO6SoJNF3VY2tnlzypHl-4w'
    #channel_id = 'UCxse2SVhmf5wzi12L6B_Wog'
    #channel_id = 'UCg4mMShkzgnIWuwTZ5uMahQ'
    channel_id = 'UCRs1pHnES3QDdh43xbjOmzw'
    channel_contents = get_channel_contents(channel_id)
    print(channel_contents)


def test_channel_list():
    channel_list = get_channel_list_by_category_language(26, 'en', 3)
    #print(channel_list)
    for channel_data in channel_list:
        print(channel_data)


def test_video_description_batch():
    video_id_list = [
        "YWn9xIGbZfw", "50Smb_N8V0A", "4llgRFENDs8", "Xiv7TnvqRWw", "EaL4O17cx0k", "Bj6y0tw436c", "WF6cf5sSWwI",
    ]
    
    video_id_description_map = get_video_description_batch(video_id_list)
    for k, v in video_id_description_map.items():
        print(k, v[:10])


def get_channel_list_by_tag(tag, size):
    channel_id_list = []
    try:
        param_dic = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        #{"term": {"languages": {"value": language}}},
                        {"term": {"tag_list.keyword": {"value": tag}}},
                        {"range": {"is_delete": {"lt": 2}}},
                    ]
                }
            },
            "sort":[
                {"total_views": {"order": "desc"}},
            ]
        }
        json_obj = es.search(index=channel_index, body=param_dic, timeout='1m')
        hits = json_obj['hits']['hits']
        channel_list = []
        for hit in hits:
            channel_id_list.append(hit['_id'])
    except:
        logging.error(traceback.format_exc())
    return channel_id_list


def test_get_channel_list_by_tag():
    tag = '경제&재테크'
    #tag = 'fitness'
    #tag = '운세'
    size = 1024
    channel_list = get_channel_list_by_tag(tag, size)
    for channel_info in channel_list:
        print(channel_info)


def test_get_channel_video_list():
    channel_id = 'UCpEVIcVrgeTgJusnsV4C5wA'
    video_list = get_channel_video_list(channel_id, 32)
    for video in video_list:
        print(video)


if __name__ == "__main__":
    test_channel_contents()        
    #test_video_description_batch()
    #test_get_channel_list_by_tag()
    #test_get_channel_video_list()





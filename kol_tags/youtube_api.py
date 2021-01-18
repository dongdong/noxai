#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: cl
@time: 2019/11/27 16:21
"""
import hashlib
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
from multiprocessing.dummy import Pool
pool_spider = redis.ConnectionPool(host='r-j6c4ywxr8378ad1dax.redis.rds.aliyuncs.com', password='dhmi5xMPjjfidUx4', port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)
import time
import datetime
import fcntl
from dateutil.relativedelta import relativedelta
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')
urllib3.disable_warnings()
from collections import Counter, defaultdict

header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
    'Connection': 'close',
    'accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
    'referer': 'https://www.youtube.com/',
}
youtube_key = 'AIzaSyBVEIDSHVx3M00_yfAXQuTVqVhc-wLPwCc'


def request_youtube_api_obj(request_url):
    response_text = requests.get(url=request_url, headers=header, verify=False).text
    json_obj = json.loads(response_text)
    if json_obj.get('error'):
        logging.warn('request error, retry...')
        time.sleep(1)
        response_text = requests.get(url=request_url, headers=header, verify=False).text
        json_obj = json.loads(response_text)
    return json_obj


def search_video_list_by_keyword(keyword, language):
    video_id_list = []
    page_size = 50
    #pages = int(max_size / page_size)
    pages = 8
    request_url = (('https://www.googleapis.com/youtube/v3/search?'
            + 'q=%s&relevanceLanguage=%s&maxResults=%d&key=%s&type=video') 
            % (keyword, language, page_size, youtube_key))
    for page in range(pages):
        print(request_url)
        json_obj = request_youtube_api_obj(request_url)
        #print(json_obj)
        next_page_token = json_obj.get('nextPageToken')
        items = json_obj.get('items')
        if items is None:
            logging.error('items is None => %s' % youtube_key)
            logging.error(json_obj)
            break
        video_ids = [item['id']['videoId'] for item in items] 
        video_id_list += video_ids
        request_url = (('https://www.googleapis.com/youtube/v3/search?' 
                + 'q=%s&relevanceLanguage=%s&maxResults=%d&pageToken=%s&key=%s&type=video') 
                % (keyword, language, page_size, next_page_token, youtube_key))
    return video_id_list


def get_video_contents(video_id, language):
    request_url = (('https://www.googleapis.com/youtube/v3/videos?id=%s&part=snippet&key=%s') 
            % (video_id, youtube_key))
    json_obj = request_youtube_api_obj(request_url)
    #print(json_obj)
    items = json_obj.get('items') 
    if items is None or len(items) <= 0:
        logging.error('items is None => %s' % youtube_key)
        logging.error(json_obj)
        return None
    snippet = items[0]['snippet']
    #print(snippet)
    data = {
        'id': video_id,
        'title': snippet.get('title', ''),
        'category': snippet.get('categoryId', ''),
        'keywords': snippet.get('tags',[]),
        'description': snippet.get('description', []),
        'language': language,
        'from': 'youtube',
    } 
    return data

def get_topic_details(respond_obj):
    data = {
        'topic_ids': [],
        'topic_categories': [], 
    }
    items = respond_obj.get('items') 
    if items is None or len(items) <= 0:
        logging.error('items is None => %s' % youtube_key)
        logging.error(respond_obj)
        return data
    item = items[0]
    if 'topicDetails' not in item:
        return data
    topic_details = item['topicDetails']
    if 'topicIds' in topic_details:
        topic_id_list = list(set(topic_details['topicIds']))
    elif 'relevantTopicIds' in topic_details:
        topic_id_list = list(set(topic_details['relevantTopicIds']))
    else:
        topic_id_list = []
    if 'topicCategories' in topic_details:
        topic_category_list = [cate[30:] for cate in topic_details['topicCategories']]
    else:
        topic_category_list = []
    data['topic_ids'] = topic_id_list
    data['topic_categories'] = topic_category_list
    return data


def get_youtube_video_topics(video_id):
    request_url = 'https://www.googleapis.com/youtube/v3/videos?id=%s&key=%s&part=topicDetails' % (video_id, youtube_key)
    json_obj = request_youtube_api_obj(request_url)
    data = get_topic_details(json_obj)
    return data
   

def get_youtube_channel_topics(channel_id):
    topics = {}
    request_url = 'https://www.googleapis.com/youtube/v3/channels?id=%s&key=%s&part=topicDetails' % (channel_id, youtube_key)
    json_obj = request_youtube_api_obj(request_url)
    #print(json_obj)
    data = get_topic_details(json_obj)
    return data


def get_youtube_video_topics_batch(video_id_list):
    ret = {}
    video_ids = ','.join([video_id for video_id in video_id_list if video_id is not None])
    request_url = 'https://www.googleapis.com/youtube/v3/videos?id=%s&key=%s&part=topicDetails' % (video_ids, youtube_key)
    json_obj = request_youtube_api_obj(request_url)
    #print(json_obj)
    items = json_obj.get('items') 
    if items is None or len(items) <= 0:
        logging.error('items is None => %s' % youtube_key)
        logging.error(json_obj)
        return ret
    for item in items:
        if 'id' in item and 'topicDetails' in item:
            video_id = item['id']
            topic_details = item['topicDetails']
            if 'relevantTopicIds' in topic_details:
                topic_id_list = list(set(topic_details['relevantTopicIds']))
                ret[video_id] = topic_id_list
    return ret




if __name__ == "__main__":
    '''
    keyword = '美妆'
    language = 'zh-Hans'
    video_id_list = search_video_list_by_keyword(keyword, language)    
    print(video_id_list)
    '''
    '''
    #video_id_list = [
    #    "YWn9xIGbZfw", "50Smb_N8V0A", "4llgRFENDs8", "Xiv7TnvqRWw", "EaL4O17cx0k", "Bj6y0tw436c", "WF6cf5sSWwI",
    #]
    video_id_list = [
        'AvVZzBk6HVk', 'ilUIaZjOTL0', '5eyw0tRurCc'
    ]
    i = 0
    for video_id in video_id_list:
        #video_id = '4WsFwKn-ufM'
        video_info = get_video_contents(video_id)
        print(i)
        print(video_info)
        i += 1
    '''

    '''
    video_id_list = [
        'AvVZzBk6HVk', 'ilUIaZjOTL0', '5eyw0tRurCc'
    ]
    for video_id in video_id_list:
        topic_details = get_youtube_video_topics(video_id)
        print(topic_details)

    channel_id = 'UClqhvGmHcvWL9w3R48t9QXQ'
    topic_details = get_youtube_channel_topics(channel_id)
    print(topic_details)
    '''

    video_id_list = [
        'AvVZzBk6HVk', 'ilUIaZjOTL0', '5eyw0tRurCc'
    ]
    video_id_topic_map = get_youtube_video_topics_batch(video_id_list)
    print(video_id_topic_map)   



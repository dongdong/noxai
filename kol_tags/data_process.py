import os
import json
import time
import logging
from collections import defaultdict
import traceback

from youtube_api import search_video_list_by_keyword
from youtube_api import get_video_contents as get_video_contents_by_youtube
from es_utils import get_video_contents as get_video_contents_by_es
from video import VideoData, VideoInfo
from tag import tag_search_keywords_config, ModelTagConfig
from tag_model import FeatureData, TrainData
import path_manager as pm
from text_processor import TFIDFModel

from gensim.models import Word2Vec, KeyedVectors

import numpy as np
from scipy.sparse import coo_matrix

from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB, ComplementNB


FEATURE_WORD_LENGTH_MIN_THRESHOLD = 10

def _get_video_list_dict(language, cache={}):
    tag_video_list_dict = {}
    tag_search_keywords_list = tag_search_keywords_config[language]
    for tag_data in tag_search_keywords_list:
        logging.info('get video id list from tag search keywords. config data: %s' % (str(tag_data)))
        level_1_tag = tag_data['level_1']
        level_2_tag = tag_data['level_2']
        search_keyword_list = tag_data['search_keywords']
        if level_1_tag not in tag_video_list_dict:
            tag_video_list_dict[level_1_tag] = {}
        if level_2_tag not in tag_video_list_dict[level_1_tag]:
            tag_video_list_dict[level_1_tag][level_2_tag] = {}
        for search_keyword in search_keyword_list:
            if (cache is not None and level_1_tag in cache 
                    and level_2_tag in cache[level_1_tag]
                    and search_keyword in cache[level_1_tag][level_2_tag]):
                video_id_list = cache[level_1_tag][level_2_tag][search_keyword] 
                logging.info('get video list. hit cache! tag: %s##%s. keywords: %s. total results: %d' % 
                        (level_1_tag, level_2_tag, search_keyword, len(video_id_list)))
            else:
                video_id_list = search_video_list_by_keyword(search_keyword, language)
                logging.info('search for tag: %s##%s. keywords: %s. total results: %d' % 
                        (level_1_tag, level_2_tag, search_keyword, len(video_id_list)))
                time.sleep(1)
            tag_video_list_dict[level_1_tag][level_2_tag][search_keyword] = video_id_list
    return tag_video_list_dict


def dump_video_list_by_tag(language, is_incremental_update=True):
    dump_path = pm.get_tag_video_list_path(language)
    if is_incremental_update and os.path.exists(dump_path):
        logging.info('incremental update tag video list for language: %s' % (language))
        with open(dump_path) as f:
            cache = json.load(f)
            tag_video_list_dict = _get_video_list_dict(language, cache)
    else:
        logging.info('dump tag video list for language: %s' % (language))
        tag_video_list_dict = _get_video_list_dict(language)
    with open(dump_path, 'w') as f:
        json.dump(tag_video_list_dict, f)
    logging.info('dump tag video list finish!')


def get_video_data_from_youtube(video_id, language):
    try:
        video_contents = get_video_contents_by_youtube(video_id, language)  
    except:
        logging.warn('get video contents fails. video_id: %s' % (video_id)) 
        video_contents = None
    video_data = None
    if video_contents is not None:
        video_data = VideoData.From_dict(video_contents)
    return video_data


def get_video_data_from_es(video_id):
    video_contents = get_video_contents_by_es(video_id)            
    video_data = None
    if video_contents is not None:
        video_data = VideoData.From_dict(video_contents)
    return video_data


def get_video_data(video_id, language):
    #    video_data = get_video_data_from_es(video_id)
    #if video_data is None:
    video_data = get_video_data_from_youtube(video_id, language)
    return video_data 


def _get_video_data_cache(video_data_path):
    cache = {}
    if os.path.exists(video_data_path):
        with open(video_data_path) as f:
            for line in f:
                video_contents = json.loads(line)
                video_data = VideoData.From_dict(video_contents)
                if video_data is not None:
                    video_id = video_data.video_id
                    cache[video_id] = video_data
    return cache


def _dump_video_data(language, video_data_dir, video_id_list, level_1_tag, level_2_tag, is_incremental_update):
    total_num = 0
    fail_num = 0
    cache_num = 0
    file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
    video_data_path = os.path.join(video_data_dir, file_name)
    if is_incremental_update:
        video_data_cache = _get_video_data_cache(video_data_path)
    else:
        video_data_cache = {}
    with open(video_data_path, 'w') as f_out:
        for video_id in video_id_list:
            total_num += 1
            if is_incremental_update and video_id in video_data_cache:
                video_data = video_data_cache[video_id]
                cache_num += 1
            else:
                video_data = get_video_data(video_id, language)
            if video_data is None:
                fail_num += 1
                continue
            video_info = VideoInfo(video_data, level_1_tag, level_2_tag)
            dump_contents = video_info.get_data()
            f_out.write(json.dumps(dump_contents) + '\n')
            if total_num % 100 == 0:
                logging.info('dump video info to %s. total: %d, cached: %d, failed: %d' % 
                    (file_name, total_num, cache_num, fail_num))
                time.sleep(1)
    return total_num, cache_num, fail_num


def dump_video_data_from_video_list(language, is_incremental_update=True):
    tag_video_id_path = pm.get_tag_video_list_path(language)
    video_data_dir = pm.get_tag_video_data_dir(language)
    logging.info('dump video data. language: %s, output dir: %s' % (language, video_data_dir))
    level_1_tag_num = 0
    level_2_tag_num = 0
    total_num = 0
    fail_num = 0
    cache_num = 0
    with open(tag_video_id_path, 'r') as f:
        tag_video_id_dict = json.load(f)
        for level_1_tag, level_2_dict in tag_video_id_dict.items():
            level_1_tag_num += 1
            for level_2_tag, keyword_dict in level_2_dict.items():
                level_2_tag_num += 1
                video_id_list = []
                for keyword, keyword_id_list in keyword_dict.items():
                     video_id_list += keyword_id_list
                video_id_list = list(set(video_id_list))
                total, cache, fail = _dump_video_data(language, video_data_dir, video_id_list, 
                        level_1_tag, level_2_tag, is_incremental_update)
                total_num += total
                cache_num += cache
                fail_num += fail
            #break #test

    logging.info('dump video data finish. level_1 tag: %d, level_2 tag: %d, total: %d, cached: %d, failed: %d' % 
            (level_1_tag_num, level_2_tag_num, total_num, cache_num, fail_num))


def _get_video_data_from_json_obj(json_obj):
    video_data = VideoData.From_dict(json_obj)
    return video_data
    

def _get_video_info_from_json_obj(json_obj):
    video_info = None
    processed_video_data = _get_video_data_from_json_obj(json_obj)
    if processed_video_data is not None:
        level_1_tag = json_obj.get('level_1')
        level_2_tag = json_obj.get('level_2')
        video_info = VideoInfo(processed_video_data, level_1_tag, level_2_tag)
    return video_info


def _get_video_info_cache(processed_video_data_path):
    cache = {}
    if os.path.exists(processed_video_data_path):
        with open(processed_video_data_path, 'r') as f:
            for line in f:
                json_obj = json.loads(line)
                video_info = _get_video_info_from_json_obj(json_obj)
                if video_info is not None:
                    video_id = video_info.video_data.video_id
                    cache[video_id] = video_info
                    #print(video_id)
    return cache


def _process_video_data_file(video_data_path, processed_video_data_path, is_incremental_update, 
        video_info_data_dict, tfidf_train_data_list, file_name):
    total_num = 0
    cache_num = 0
    fail_num = 0
    logging.info("process video data, file path: %s." % video_data_path)
    
    if is_incremental_update:
        video_info_cache = _get_video_info_cache(processed_video_data_path)
        logging.info('load process video data cache from %s, size: %d' % 
                    (processed_video_data_path, len(video_info_cache.keys())))
    else:
        video_info_cache = {}

    video_info_data_dict[file_name] = []
    with open(video_data_path, 'r') as f_in:
        for line in f_in:
            total_num += 1
            json_obj = json.loads(line)
            video_id = json_obj.get('id', '')
            if is_incremental_update and video_id in video_info_cache:
                video_info = video_info_cache[video_id]
                cache_num += 1
            else:
                video_info = _get_video_info_from_json_obj(json_obj)
                if video_info is None:
                    fail_num += 1
                    continue
                video_info.video_data.parse_text()
            feature_words = video_info.video_data.get_feature_words()
            tfidf_train_data_list.append(feature_words)
            video_info_data_dict[file_name].append(video_info)
       
    logging.info("process video data %s finish. total: %d, cached: %d, failed: %d" % 
            (file_name, total_num, cache_num, fail_num))
    return total_num, cache_num, fail_num


def _process_video_data(video_data_dir, processed_video_data_dir, is_incremental_update, model_tag_config):
    logging.info("process video data, dir: %s." % video_data_dir)
    tfidf_train_data_list = []
    video_info_data_dict = {}
    cache_num_sum = 0
    total_num_sum = 0
    fail_num_sum = 0

    #for file_name in os.listdir(video_data_dir):
    for level_1_tag, level_2_tag in model_tag_config.get_tag_iter():
        file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
        video_data_path = os.path.join(video_data_dir, file_name)
        processed_video_data_path = os.path.join(processed_video_data_dir, file_name)
    
        if not os.path.exists(video_data_path):
            logging.error('Video data file to be processed not found!!! file name: %s' % (file_name)) 
            continue       

        total_num, cache_num, fail_num = _process_video_data_file(video_data_path, processed_video_data_path, 
                is_incremental_update, video_info_data_dict, tfidf_train_data_list, file_name)
        total_num_sum += total_num
        cache_num_sum += cache_num
        fail_num_sum += fail_num
        #break #test

    logging.info("process video data finish. total: %d, cached: %d, failed: %d" % 
            (total_num_sum, cache_num_sum, fail_num_sum))
    return tfidf_train_data_list, video_info_data_dict
        

def _dump_processed_video_data(processed_video_data_dir, video_info_data_dict, tfidf_model):
    logging.info("dump processed video data, output dir: %s." % processed_video_data_dir)
    
    for file_name, video_info_list in video_info_data_dict.items():
        processed_video_data_path = os.path.join(processed_video_data_dir, file_name)
        logging.info("dump processed video data, file path: %s." % processed_video_data_path)
        
        with open(processed_video_data_path, 'w') as f_out:
            for video_info in video_info_list:
                video_info.video_data.process_tfidf_words(tfidf_model)
                dump_data = video_info.get_data()
                f_out.write(json.dumps(dump_data) + '\n')
                

def dump_processed_video_data(language, is_incremental_update=True):
    video_data_dir = pm.get_tag_video_data_dir(language)
    processed_video_data_dir = pm.get_tag_processed_video_data_dir(language)
    logging.info("dump processed video data, input dir: %s." % video_data_dir)

    model_tag_config = ModelTagConfig.Create(language)
    tfidf_train_data_list, video_info_data_dict = _process_video_data(video_data_dir, 
            processed_video_data_dir, is_incremental_update, model_tag_config)
    logging.info('process video contents finish. data size:%d' % len(tfidf_train_data_list))

    tfidf_model_dir = pm.get_tfidf_train_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    tfidf_model.train(tfidf_train_data_list)
    tfidf_model.save_model()
    logging.info('train tfidf model finish. model dir:%s' % (tfidf_model_dir))

    logging.info("dump processed video data, output dir: %s." % processed_video_data_dir)
    _dump_processed_video_data(processed_video_data_dir, video_info_data_dict, tfidf_model)
    

def get_feature_data_from_video_info_iter(video_info_iter, tfidf_model, is_train=False):
    #logging.info("get model data from video tag iter.")
    i = 0
    num_pos = tfidf_model.get_dictionary_size()
    data = []
    row = []
    col = []
    tag_list = []
    valid_video_id_list = []

    for video_info in video_info_iter:
        feature_words = video_info.video_data.get_feature_words()
        if len(feature_words) < FEATURE_WORD_LENGTH_MIN_THRESHOLD:
            logging.info('drop data, feature too small. %s' % (feature_words))
            continue
        else:
            valid_video_id_list.append(video_info.video_data.video_id)

        tfidf_vec = tfidf_model.get_vector(feature_words)
        for pos, score in tfidf_vec:
            data.append(score)
            row.append(i)
            col.append(pos)
        
        if is_train:
            level_1 = video_info.level_1_tag
            level_2 = video_info.level_2_tag
            tag_list.append((level_1, level_2))
        i += 1

    logging.info("process feature data finish. X data size: %d, X data shape: (%d, %d), y data size: %d" % 
            (len(data), i, num_pos, len(tag_list)))
    feature_data = FeatureData(data, row, col, i, num_pos, tag_list)
    return feature_data, valid_video_id_list


def _get_video_info_iter_from_processed_video_data_dir(processed_video_data_dir, model_tag_config):
    logging.info("get processed video data iter from processed video data dir: %s." % processed_video_data_dir)

    for level_1_tag, level_2_tag in model_tag_config.get_tag_iter():
        file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
        processed_video_data_path = os.path.join(processed_video_data_dir, file_name)
        logging.info("process train data, file path: %s." % processed_video_data_path)

        if os.path.exists(processed_video_data_path):
            with open(processed_video_data_path, 'r') as f:
                for line in f:
                    json_obj = json.loads(line)
                    video_info = _get_video_info_from_json_obj(json_obj)
                    if video_info is not None:
                        yield video_info
        else:
            logging.warn('file not found! path: %s' % processed_video_data_path)


def _get_feature_data_from_processed_video(processed_video_data_dir, tfidf_model, model_tag_config):
    logging.info("process train data, from dir: %s." % processed_video_data_dir)
    
    video_info_iter = _get_video_info_iter_from_processed_video_data_dir(processed_video_data_dir, model_tag_config)
    feature_data, _ = get_feature_data_from_video_info_iter(video_info_iter, tfidf_model, True)
    return feature_data


def dump_feature_data(language):
    tfidf_model_dir = pm.get_tfidf_train_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    if not tfidf_model.load_model():
        logging.error('Failed to load tfidf model, return')
        return

    model_tag_config = ModelTagConfig.Create(language)
    processed_video_data_dir = pm.get_tag_processed_video_data_dir(language)
    feature_data = _get_feature_data_from_processed_video(processed_video_data_dir, tfidf_model, model_tag_config)

    feature_data_path = pm.get_tag_video_feature_data_path(language)
    feature_data.save(feature_data_path)


if __name__ == '__main__':
    language = 'en'
    #language = 'zh-Hans'
    #dump_video_list_by_tag(language)
    #dump_video_data_from_video_list(language)
    #dump_video_data_from_video_list(language, False)
    #dump_processed_video_data(language, False)
    #dump_processed_video_data(language)
    dump_feature_data(language)


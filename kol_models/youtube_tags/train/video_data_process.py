import os
import json
import logging
import time
import argparse

import kol_models.youtube_tags.commons.path_manager as pm
from kol_models.commons.youtube_api import get_video_contents 
from kol_models.youtube_tags.commons.video_data import VideoData, parse_video_text
from kol_models.youtube_tags.commons.tag_data import TrainTagData
from kol_models.youtube_tags.commons.text_processor import TFIDFModel
from kol_models.youtube_tags.train.tag_video_config import TagVideoConfig


def _get_video_data(video_id):
    video_data = None
    video_data_dict = get_video_contents(video_id)
    if video_data_dict:
        video_data = VideoData(video_data_dict)
    return video_data


def _load_video_data_from_json(text):
    video_data = None
    try:
        json_obj = json.loads(text)
        if json_obj:
            video_data = VideoData(json_obj)
    except:
        logging.warn('parse json text error! contents: ' + text)
    return video_data


def get_video_data_dict_from_file(video_data_path):
    video_data_dict = {}
    if os.path.exists(video_data_path):
        with open(video_data_path) as f:
            for line in f:
                video_data = _load_video_data_from_json(line)
                if video_data:
                    video_id = video_data.video_id
                    video_data_dict[video_id] = video_data
    logging.info('load video data dict from %s. total num: %d' 
            % (video_data_path, len(video_data_dict.keys())))
    return video_data_dict


def iter_video_data_from_file(video_data_path):
    with open(video_data_path, 'r') as f:
        for line in f:
            video_data = _load_video_data_from_json(line)
            if video_data is not None:
                yield video_data


def _load_train_tag_data_from_json(text):
    train_tag_data = None
    try:
        json_obj = json.loads(text)
        if json_obj:
            train_tag_data = TrainTagData.From_dict(json_obj)
    except:
        logging.warn('parse json text error! contents: ' + text)
    return train_tag_data


def get_train_tag_data_dict_from_file(train_tag_data_path):
    train_tag_data_dict = {}
    if os.path.exists(train_tag_data_path):
        with open(train_tag_data_path, 'r') as f:
            for line in f:
                train_tag_data = _load_train_tag_data_from_json(line)
                if train_tag_data:
                     train_tag_data_dict[train_tag_data.item_id] = train_tag_data
    logging.info('load train tag data dict from %s. total num: %d' 
            % (train_tag_data_path, len(train_tag_data_dict.keys())))
    return train_tag_data_dict


def _dump_train_video_data(level_1_tag, level_2_tag, video_id_list, video_data_dir, 
        is_incremental_update):
    total_num = 0
    fail_num = 0
    cached_num = 0
    tag_file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
    video_data_path = os.path.join(video_data_dir, tag_file_name)
    if is_incremental_update:
        video_data_cache = get_video_data_dict_from_file(video_data_path)
    else:
        video_data_cache = {}
    video_data_list = []
    for video_id in video_id_list:
        total_num += 1
        if is_incremental_update and video_id in video_data_cache:
            video_data = video_data_cache[video_id]
            cached_num += 1
        else:
            video_data = _get_video_data(video_id)
        if video_data is not None:
            video_data_list.append(video_data)
        else:
            fail_num += 1
        if total_num % 100 == 0:
            logging.info('dump video data to %s. total: %d, cached: %d, failed: %d' 
                % (tag_file_name, total_num, cached_num, fail_num))
            time.sleep(1)
    if video_data_list:
        with open(video_data_path, 'w') as f_out:
            for video_data in video_data_list:
                dump_text = json.dumps(video_data.get_data())
                f_out.write(dump_text + '\n')
    logging.info('dump video data to %s finish. total: %d, cached: %d, failed: %d' 
            % (tag_file_name, total_num, cached_num, fail_num))
    return total_num, cached_num, fail_num


def dump_video_data(language, is_incremental_update=True):
    tag_video_config = TagVideoConfig.Load(language)
    video_data_dir = pm.get_train_video_data_dir(language)
    logging.info('dump video data. language: %s, output dir: %s' % (language, video_data_dir))
    dump_stats = {
        'level_1_tag_num': 0,
        'level_2_tag_num': 0,
        'total_video_num': 0,
        'fail_video_num': 0,
        'cached_video_num': 0,
    }
    for level_1_tag in tag_video_config.get_level_1_tags():
        dump_stats['level_1_tag_num'] += 1
        for level_2_tag in tag_video_config.get_level_2_tags(level_1_tag):
            dump_stats['level_2_tag_num'] += 1
            video_id_list = tag_video_config.get_video_id_list(level_1_tag, level_2_tag)
            total, cached, fail = _dump_train_video_data(level_1_tag, level_2_tag, video_id_list, 
                    video_data_dir, is_incremental_update)
            dump_stats['total_video_num'] += total
            dump_stats['cached_video_num'] += cached
            dump_stats['fail_video_num'] += fail
        #break #test
    logging.info(('dump search video data finish. level_1 tag: %d, level_2 tag: %d,' 
            + ' total: %d, cached: %d, failed: %d') 
            % (dump_stats['level_1_tag_num'], dump_stats['level_2_tag_num'], 
                dump_stats['total_video_num'], dump_stats['cached_video_num'], 
                dump_stats['fail_video_num']))


def _get_train_tag_data(video_data, language):
    parse_video_text(video_data, language)
    train_tag_data = TrainTagData.From_video_data(video_data)
    return train_tag_data


def _process_train_tag_data_from_video(video_data_path, train_tag_data_path, 
        train_tag_data_cache, train_tag_data_list, tfidf_train_data_list, language):
    total_num = 0
    cache_num = 0
    fail_num = 0
    logging.info("process video data data, file path: %s." % video_data_path)
    video_data_dict = get_video_data_dict_from_file(video_data_path)
    for video_id, video_data in video_data_dict.items():
        total_num += 1
        if video_id in train_tag_data_cache:
            train_tag_data = train_tag_data_cache[video_id]
            cache_num += 1
        else:
            train_tag_data = _get_train_tag_data(video_data, language)
            if not train_tag_data.feature_words:
                logging.warn('fail to get feature words. video data: %s' % str(video_data.get_data()))
                fail_num += 1
                continue
        train_tag_data_list.append(train_tag_data)
        tfidf_train_data_list.append(train_tag_data.feature_words)
    logging.info("process train video data %s finish. total: %d, cached: %d, failed: %d" 
            % (video_data_path, total_num, cache_num, fail_num))
    return total_num, cache_num, fail_num


def _dump_train_tag_data(language, is_incremental_update):
    tag_video_config = TagVideoConfig.Load(language)
    video_data_dir = pm.get_train_video_data_dir(language)
    train_tag_data_path = pm.get_train_tag_data_path(language)
    tfidf_train_data_list = []
    process_stats ={
        'total_video_num': 0,
        'cached_video_num': 0,
        'fail_video_num': 0,
    }
    train_tag_data_cache = {}
    if is_incremental_update:
        train_tag_data_cache = get_train_tag_data_dict_from_file(train_tag_data_path)
    train_tag_data_list = []
    for level_1_tag, level_2_tag in tag_video_config.iter_tags():
        file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
        video_data_path = os.path.join(video_data_dir, file_name)
        assert os.path.exists(video_data_path)
        total_num, cache_num, fail_num = _process_train_tag_data_from_video(
                video_data_path, train_tag_data_path, train_tag_data_cache, 
                train_tag_data_list, tfidf_train_data_list, language)
        process_stats['total_video_num'] += total_num
        process_stats['cached_video_num'] += cache_num
        process_stats['fail_video_num'] += fail_num
        #break #test
    logging.info("process video data finish. total: %d, cached: %d, failed: %d" 
            % (process_stats['total_video_num'], process_stats['cached_video_num'], 
            process_stats['fail_video_num']))
    if train_tag_data_list:
        logging.info('dump train tag data. total: %d' % len(train_tag_data_list))
        with open(train_tag_data_path, 'w') as f:
            for train_tag_data in train_tag_data_list:
                dump_data = json.dumps(train_tag_data.get_data())
                f.write(dump_data + '\n')
    return tfidf_train_data_list
 

def _train_tfidf_model(language, tfidf_train_data_list):
    tfidf_model_dir = pm.get_train_tfidf_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    tfidf_model.train(tfidf_train_data_list)
    tfidf_model.save_model()
    logging.info('train tfidf model finish. model dir:%s' % (tfidf_model_dir))


def dump_train_tag_data(language, is_incremental_update=True):
    logging.info("dump train tag data, language: %s." % language)
    tfidf_train_data_list = _dump_train_tag_data(language, is_incremental_update)
    _train_tfidf_model(language, tfidf_train_data_list)


def main(args):
    def str2bool(bool_str):
        return False if bool_str.lower() == 'false' else True
    #print(args) 
    cmd = args['cmd']
    language = args['lang']
    is_incremental_update = str2bool(args['use_cache'])
    logging.info('video data process. cmd: %s, language: %s, use cache? %s' 
            % (cmd, language, is_incremental_update))
    if cmd == 'dump-video-data': 
        dump_video_data(language, is_incremental_update)
    elif cmd == 'dump-train-tag-data':
        dump_train_tag_data(language, is_incremental_update)
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'dump-video-data',
            'dump-train-tag-data',
        },
        required=True,
        help='language',
    ) 
    parser.add_argument(
        '--lang',
        choices={
            'en',
            'zh',
            'ko',
            'ja',
        },
        required=True,
        help='language',
    ) 
    parser.add_argument(
        '--use-cache',
        default='True',
        help='use cache or not',
    ) 
    args = parser.parse_args()
    arguments = args.__dict__
    main(arguments)




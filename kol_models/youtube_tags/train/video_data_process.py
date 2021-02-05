import os
import json
import logging
import time
import argparse

import kol_models.youtube_tags.commons.path_manager as pm
from kol_models.commons.youtube_api import get_video_contents 
from kol_models.youtube_tags.commons.video_data import VideoData, parse_video_text
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


def _dump_tag_video_data(level_1_tag, level_2_tag, video_id_list, video_data_dir, is_incremental_update):
    total_num = 0
    fail_num = 0
    cached_num = 0
    tag_file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
    video_data_path = os.path.join(video_data_dir, tag_file_name)
    if is_incremental_update:
        video_data_cache = get_video_data_dict_from_file(video_data_path)
    else:
        video_data_cache = {}
    with open(video_data_path, 'w') as f_out:
        for video_id in video_id_list:
            total_num += 1
            if is_incremental_update and video_id in video_data_cache:
                video_data = video_data_cache[video_id]
                cached_num += 1
            else:
                video_data = _get_video_data(video_id)
            if video_data is None:
                fail_num += 1
                continue
            dump_text = json.dumps(video_data.get_data())
            f_out.write(dump_text + '\n')
            if total_num % 100 == 0:
                logging.info('dump video data to %s. total: %d, cached: %d, failed: %d' 
                        % (tag_file_name, total_num, cached_num, fail_num))
                time.sleep(1)
    logging.info('dump video data to %s finish. total: %d, cached: %d, failed: %d' 
            % (tag_file_name, total_num, cached_num, fail_num))
    return total_num, cached_num, fail_num


def dump_video_data(language, is_incremental_update=True):
    tag_video_config = TagVideoConfig.Load(language)
    video_data_dir = pm.get_tag_video_dir(language)
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
            total, cached, fail = _dump_tag_video_data(level_1_tag, level_2_tag, video_id_list, 
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


def _get_processed_video_data(video_data, language):
    parse_video_text(video_data, language)
    return video_data


def _dump_processed_tag_video_data(video_data_path, processed_video_data_path, 
        processed_video_data_cache, tfidf_train_data_list, language):
    total_num = 0
    cache_num = 0
    fail_num = 0
    logging.info("process video data, file path: %s." % video_data_path)
    video_data_dict = get_video_data_dict_from_file(video_data_path)
    with open(processed_video_data_path, 'w') as f:
        #for video_data in iter_video_data_from_file(video_data_path):
        for video_id, video_data in video_data_dict.items():
            total_num += 1
            #video_id = video_data.video_id
            if video_id in processed_video_data_cache:
                processed_video_data = processed_video_data_cache[video_id]
                cache_num += 1
            else:
                processed_video_data = _get_processed_video_data(video_data, language)
                if not processed_video_data.feature_words:
                    logging.warn('fail to get feature words. video data: %s' % str(video_data.get_data()))
                    fail_num += 1
                    continue
            tfidf_train_data_list.append(processed_video_data.feature_words)
            dump_data = json.dumps(processed_video_data.get_data())
            f.write(dump_data + '\n')
    logging.info("process video data %s finish. total: %d, cached: %d, failed: %d" 
            % (video_data_path, total_num, cache_num, fail_num))
    return total_num, cache_num, fail_num


def _dump_processed_video_data(language, is_incremental_update):
    tag_video_config = TagVideoConfig.Load(language)
    video_data_dir = pm.get_tag_video_dir(language)
    processed_video_data_dir = pm.get_tag_processed_data_dir(language)
    tfidf_train_data_list = []
    process_stats ={
        'total_video_num': 0,
        'cached_video_num': 0,
        'fail_video_num': 0,
    }
    for level_1_tag, level_2_tag in tag_video_config.iter_tags():
        file_name = pm.get_video_data_file_name(level_1_tag, level_2_tag)
        video_data_path = os.path.join(video_data_dir, file_name)
        processed_video_data_path = os.path.join(processed_video_data_dir, file_name)
        assert os.path.exists(video_data_path)
        processed_video_data_cache = {}
        if is_incremental_update:
            processed_video_data_cache = get_video_data_dict_from_file(processed_video_data_path)
        total_num, cache_num, fail_num = _dump_processed_tag_video_data(video_data_path, 
                processed_video_data_path, processed_video_data_cache, tfidf_train_data_list,
                language)
        process_stats['total_video_num'] += total_num
        process_stats['cached_video_num'] += cache_num
        process_stats['fail_video_num'] += fail_num
        #break #test
    logging.info("process video data finish. total: %d, cached: %d, failed: %d" 
            % (process_stats['total_video_num'], process_stats['cached_video_num'], 
            process_stats['fail_video_num']))
    return tfidf_train_data_list
 

def _train_tfidf_model(language, tfidf_train_data_list):
    tfidf_model_dir = pm.get_train_tfidf_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    tfidf_model.train(tfidf_train_data_list)
    tfidf_model.save_model()
    logging.info('train tfidf model finish. model dir:%s' % (tfidf_model_dir))


def dump_processed_video_data(language, is_incremental_update=True):
    logging.info("dump processed video data, language: %s." % language)
    tfidf_train_data_list = _dump_processed_video_data(language, is_incremental_update)
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
    elif cmd == 'dump-processed-video-data':
        dump_processed_video_data(language, is_incremental_update)
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'dump-video-data',
            'dump-processed-video-data',
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




import argparse
import os
import json
import logging
import time

from kol_models.commons.youtube_api import search_video_list_by_keyword
from kol_models.youtube_tags.commons.path_manager import get_search_video_list_path 

from kol_models.youtube_tags.config.search_config.search_config_en import en_search_keywords_list 
from kol_models.youtube_tags.config.search_config.search_config_zh import zh_search_keywords_list 
from kol_models.youtube_tags.config.search_config.search_config_ko import ko_search_keywords_list 
from kol_models.youtube_tags.config.search_config.search_config_ja import ja_search_keywords_list 

_search_keywords_config = {
    'en': en_search_keywords_list,
    'zh': zh_search_keywords_list,
    'ko': ko_search_keywords_list,
    'ja': ja_search_keywords_list,
}

def _get_tag_video_id_list_dict(language, cache={}):
    tag_video_list_dict = {}
    tag_search_keywords_list = _search_keywords_config[language]
    for tag_data in tag_search_keywords_list:
        logging.info('get video id list from tag search keywords. config data: %s' 
                % (str(tag_data)))
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
                logging.info(('get video list. hit cache! tag: %s##%s. ' 
                        + 'keywords: %s. total results: %d') 
                        % (level_1_tag, level_2_tag, search_keyword, len(video_id_list)))
            else:
                video_id_list = search_video_list_by_keyword(search_keyword, language)
                logging.info('search for tag: %s##%s. keywords: %s. total results: %d' % 
                        (level_1_tag, level_2_tag, search_keyword, len(video_id_list)))
                time.sleep(1)
            tag_video_list_dict[level_1_tag][level_2_tag][search_keyword] = video_id_list
            #break # for test
    return tag_video_list_dict


def dump_search_video_id_list(language, is_incremental_update=True):
    dump_path = get_search_video_list_path(language)
    cache = {}
    if is_incremental_update and os.path.exists(dump_path):
        logging.info('incremental update tag video list for language: %s' % (language))
        with open(dump_path) as f:
            cache = json.load(f)
    tag_video_list_dict = _get_tag_video_id_list_dict(language, cache)
    with open(dump_path, 'w') as f:
        json.dump(tag_video_list_dict, f)
    logging.info('dump tag video list finish!')


def main(args):
    def str2bool(bool_str):
        return False if bool_str.lower() == 'false' else True
    #print(args) 
    language = args['lang']
    is_incremental_update = str2bool(args['use_cache'])
    logging.info('search and dump video ids by keyword config. %s, %s.' 
            % (language, is_incremental_update))
    dump_search_video_id_list(language, is_incremental_update)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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

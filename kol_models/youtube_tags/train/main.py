import argparse
import logging

from kol_models.youtube_tags.train.search_data_process import dump_search_video_id_list
from kol_models.youtube_tags.train.video_data_process import dump_video_data, dump_processed_video_data
from kol_models.youtube_tags.train.feature_data_process import dump_tfidf_feature_data        
from kol_models.youtube_tags.models.tag_model import train_tag_model 


def do_search(language, is_incremental_update):
    logging.info('search video ids from config. language: %s, use cache: %s' 
            % (language, is_incremental_update))
    dump_search_video_id_list(language, is_incremental_update)


def do_process(language, is_incremental_update):
    logging.info('download and process videos. language: %s, use cache: %s'
            % (language, is_incremental_update))
    dump_video_data(language, is_incremental_update)
    dump_processed_video_data(language, is_incremental_update)


def do_train(language):
    logging.info('generate feature data and train tag model. language: %s'
            % (language))
    dump_tfidf_feature_data(language)
    train_tag_model(language)


def main(args):
    def str2bool(bool_str):
        return False if bool_str.lower() == 'false' else True
    #print(args)
    language = args['lang']
    cmd = args['cmd']
    is_incremental_update = str2bool(args['use_cache'])
    if (cmd == 'search' or cmd == 'search-process' 
            or cmd == 'search-process-train'
            or cmd == 'all'):
        do_search(language, is_incremental_update)
    if (cmd == 'process' or cmd == 'search-process'
            or cmd == 'process-train'
            or cmd == 'search-process-train'
            or cmd == 'all'):
        do_process(language, is_incremental_update)
    if (cmd == 'train' or cmd == 'process-train'
            or cmd == 'search-process-train'
            or cmd == 'all'):
        do_train(language)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--lang',
        choices={'en', 'zh', 'ko', 'ja'},
        required=True,
        help='language',
    ) 
    parser.add_argument(
        '--cmd',
        choices={
            'search', 
            'search-process',
            'process', 
            'process-train', 
            'train',
            'search-process-train',
            'all',
        },
        required=True,
        help='language',
    ) 
    parser.add_argument(
        '--use-cache',
        default='True',
        help='use cache or not',
    ) 
    ''' 
    parser.add_argument(
        '--search',
        action='store_true',
        help='search video ids from config',
    ) 
    parser.add_argument(
        '--process',
        action='store_true',
        help='download and process videos',
    ) 
    parser.add_argument(
        '--train',
        action='store_true',
        help='generate feature data and train tag model',
    ) 
    '''
    args = parser.parse_args()
    arguments = args.__dict__
    main(arguments)




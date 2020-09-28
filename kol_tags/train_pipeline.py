from data_process import dump_video_list_by_tag
from data_process import dump_video_data_from_video_list
from data_process import dump_processed_video_data
from data_process import dump_feature_data
from tag_model import train_tag_model 

import argparse
import logging

def run_incremental_pipeline(language):
    dump_video_list_by_tag(language)
    dump_video_data_from_video_list(language)
    dump_processed_video_data(language)
    dump_feature_data(language)
    train_tag_model(language)


def _run_pipeline(language, is_incremental_update):
    dump_video_list_by_tag(language, is_incremental_update)
    dump_video_data_from_video_list(language, is_incremental_update)
    dump_processed_video_data(language, is_incremental_update)
    dump_feature_data(language)
    train_tag_model(language)


def run_train_pipeline(arguments):
    logging.info('run train pipeline, arguments: %s' % (str(arguments)))
    language = arguments['language']
    date_str = arguments['date']
    is_incremental_update = arguments['update_type'] == 'incremental'

    _run_pipeline(language, is_incremental_update)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--language',
        help = 'language',
        required = True,
    ) 
    parser.add_argument(
        '--date',
        help = 'date str',
        required = True,
    ) 
    parser.add_argument(
        '--update_type',
        help = 'update type, full or incremental',
        required = False,
        default = 'incremental',
    )
    args = parser.parse_args()
    arguments = args.__dict__
    run_train_pipeline(arguments)




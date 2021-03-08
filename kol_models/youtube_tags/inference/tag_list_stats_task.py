import os
import time
import argparse
import logging
from collections import Counter

from kol_models.commons.pipe_engine import ChannelIdPipe
from kol_models.commons.mysql_utils import iter_channel_ids
from kol_models.commons.es_utils import get_channel_contents

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_record_log_dir = os.path.join(_cur_dir, 'pipe_record_logs')
_pipe_name = "_ytb_tag_list_stats_cid.test"
_channel_id_pipe = ChannelIdPipe(_pipe_name, _record_log_dir)

FEATURE_WORD_STAT_SIZE = 50000


def _get_channel_id_iter():
    sql = 'select cid from kol_channel_base where sub > 1000 and lang = "en"' #limit 1000000'
    logging.info('get channel id iter from database. sql: %s' % sql)
    channel_id_iter = iter_channel_ids(sql)
    return channel_id_iter


def get_channel_tag_list_by_id(channel_id):
    channel_tag_list = []
    #channel_data = ChannelData.Load_from_es(channel_id, 0)
    try:
        channel_contents = get_channel_contents(channel_id) 
        if channel_contents:
            channel_tag_list = channel_contents.get('tag_list', [])
            logging.info('get channel tag list. channel_id: %s, tag list: %s'
                    % (channel_id, channel_tag_list)) 
    except Exception as e:
        logging.error('get channel tag list error. Exception: %s' % str(e))
    return channel_tag_list


def stat_tag_list(channel_id_iter):
    word_counter = Counter()
    batch_size = 50
    total_count = 0
    valid_count = 0
    for channel_id in channel_id_iter:
        if total_count % batch_size == 0:
            logging.info('stat tag list. processed channels: %d, valid: %d' 
                    % (total_count, valid_count))
            time.sleep(1)
        total_count += 1
        tag_list = get_channel_tag_list_by_id(channel_id)
        if tag_list:
            valid_count += 1
            word_counter.update(tag_list)
    logging.info('stat channel tag list finish. total channels: %d, valid: %d' 
                % (total_count, valid_count))
    for word, count in word_counter.most_common(FEATURE_WORD_STAT_SIZE):
        print(word, count)


def set_pipe():
    channel_id_iter = _get_channel_id_iter()
    _channel_id_pipe.reset(channel_id_iter)


def append_pipe():
    channel_id_iter = _get_channel_id_iter()
    _channel_id_pipe.append(channel_id_iter)
    

def watch_pipe(interval_secs=60):
    _channel_id_pipe.watch_length()


def clean_pipe():
    cur_len = _channel_id_pipe.get_length()
    logging.info('clean pipe. current length: %d' % cur_len)
    _channel_id_pipe.clean()
    cur_len = _channel_id_pipe.get_length()
    logging.info('clean pipe finish. current length: %d' % cur_len)


def stat_tag_list_from_pipe():
    stat_tag_list(_channel_id_pipe.iter_items())


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'stat-tag-list-from-pipe':
        stat_tag_list_from_pipe()
    elif cmd == 'watch-pipe':
        watch_pipe()
    elif cmd == 'set-pipe':
        set_pipe()
    elif cmd == 'append-pipe':
        append_pipe()
    elif cmd == 'clean-pipe':
        clean_pipe()
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'stat-tag-list-from-pipe', 
            'watch-pipe',
            'set-pipe',
            'append-pipe',
            'clean-pipe',
        },
        required=True,
        help = 'run cmd type',
    ) 
    args = parser.parse_args()
    arguments = args.__dict__
    main(arguments)

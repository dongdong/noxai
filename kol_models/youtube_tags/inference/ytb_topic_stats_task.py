import os
import time
import argparse
import logging
from collections import Counter

from kol_models.commons.pipe_engine import ChannelIdPipe
from kol_models.commons.mysql_utils import iter_channel_ids
from kol_models.youtube_tags.commons.channel_data import ChannelData
from kol_models.youtube_tags.models.youtube_topic_tags import inference_tags 

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_record_log_dir = os.path.join(_cur_dir, 'pipe_record_logs')
_pipe_name = "_ytb_topic_tag_stats_cid.test"
_channel_id_pipe = ChannelIdPipe(_pipe_name, _record_log_dir)


def _get_channel_id_iter():
    #sql = 'select cid from kol_channel_base where sub <= 3000 and sub > 1000'
    #sql = 'select cid from kol_channel_base where sub > 5000 and lang = "ja" limit 10'
    sql = 'select cid from kol_channel_base where sub > 5000 limit 50000'
    #sql = 'select cid from kol_channel_base where sub > 5000 limit 50'
    channel_id_iter = iter_channel_ids(sql)
    return channel_id_iter


def stat_ytb_topics(channel_id_iter):
    topic_counter = Counter()
    batch_size = 50
    total_count = 0
    valid_count = 0
    for channel_id in channel_id_iter:
        if total_count % batch_size == 0:
            logging.info('stat youtube topics. processed channels: %d, valid: %d' 
                    % (total_count, valid_count))
            time.sleep(1)
        total_count += 1
        channel_data = ChannelData.Load_from_es(channel_id)
        if channel_data:
            tag_data_list = channel_data.tag_data_list
            ytb_tag_dict = inference_tags(tag_data_list)
            ytb_topics = []
            for item_id, item_ytb_topics in ytb_tag_dict.items():
                ytb_topics += item_ytb_topics
            if ytb_topics:
                valid_count += 1
                topic_counter.update(ytb_topics)
    logging.info('stat youtube topics finish. total channels: %d, valid: %d' 
                % (total_count, valid_count))
    for word, count in topic_counter.items():
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
    _channel_id_pipe.clean()


def stat_ytb_topics_from_pipe():
    stat_ytb_topics(_channel_id_pipe.iter_items())


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'stat-ytb-topics-from-pipe':
        stat_ytb_topics_from_pipe()
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
            'stat-ytb-topics-from-pipe', 
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

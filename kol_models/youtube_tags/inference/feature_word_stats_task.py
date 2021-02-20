import os
import time
import argparse
import logging
from collections import Counter

from kol_models.commons.pipe_engine import ChannelIdPipe
from kol_models.commons.mysql_utils import iter_channel_ids
from kol_models.youtube_tags.commons.channel_data import ChannelData

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_record_log_dir = os.path.join(_cur_dir, 'pipe_record_logs')
_pipe_name = "_ytb_feature_word_stats_cid.test"
_channel_id_pipe = ChannelIdPipe(_pipe_name, _record_log_dir)

FEATURE_WORD_STAT_SIZE = 20000


def _get_channel_id_iter():
    #sql = 'select cid from kol_channel_base where sub <= 3000 and sub > 1000'
    #sql = 'select cid from kol_channel_base where sub > 5000 and lang = "ja" limit 10'
    sql = ('select cid from kol_channel_base where sub > 3000 and '
            + 'lang != "en" and lang != "ja" and lang != "ko" and '
            + 'lang != "zh" and lang != "zh-Hant" and lang != "zh-Hans" '
            + 'limit 200000')
    channel_id_iter = iter_channel_ids(sql)
    return channel_id_iter


def _dump_channel_video_data(channel_data):
    print('*' * 20)
    print('Channel: ', channel_data.channel_id)
    print('language: ', channel_data.languages)
    count = 0
    for video_data in channel_data.video_data_list:
        count += 1
        print('#%d' % count)
        for k, v in video_data.get_data().items():
            print(k, ': ', v)
        print('-' * 20)


def get_channel_feature_words_by_id(channel_id):
    channel_feature_words = []
    channel_data = ChannelData.Load_from_es(channel_id)
    if channel_data is not None:
        #_dump_channel_video_data(channel_data)
        channel_feature_words = channel_data.feature_words
        logging.info('get channel feature words. channel_id: %s, feature words: %s'
                % (channel_id, channel_feature_words)) 
    return channel_feature_words


def stat_feature_words(channel_id_iter):
    word_counter = Counter()
    batch_size = 50
    total_count = 0
    valid_count = 0
    for channel_id in channel_id_iter:
        if total_count % batch_size == 0:
            logging.info('stat feature words. processed channels: %d, valid: %d' 
                    % (total_count, valid_count))
            time.sleep(1)
        total_count += 1
        feature_words = get_channel_feature_words_by_id(channel_id)
        if feature_words:
            valid_count += 1
            word_counter.update(feature_words)
    logging.info('stat feature words finish. total channels: %d, valid: %d' 
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
    _channel_id_pipe.clean()


def stat_feature_words_from_pipe():
    stat_feature_words(_channel_id_pipe.iter_items())


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'stat-feature-words-from-pipe':
        stat_feature_words_from_pipe()
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
            'stat-feature-words-from-pipe', 
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

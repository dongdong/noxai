import os
import time
import argparse
import logging

from kol_models.youtube_fans.age_gender_inference import update_age_gender_dist
from kol_models.commons.pipe_engine import ChannelIdPipe
from kol_models.commons.mysql_utils import iter_channel_ids

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_record_log_dir = os.path.join(_cur_dir, 'pipe_record_logs')
_pipe_name = "_age_gender_dist_cid.test"
_channel_id_pipe = ChannelIdPipe(_pipe_name, _record_log_dir)


def _get_channel_id_iter():
    #sql = 'select cid from kol_channel_base where sub <= 3000 and sub > 1000'
    sql = 'select cid from kol_channel_base where sub > 1000 limit 100'
    channel_id_iter = iter_channel_ids(sql)
    return channel_id_iter
   

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


def update_age_gender_dist_from_pipe(batch_size=100, max_wait_secs=60):
    total_num = 0
    for channel_id in _channel_id_pipe.iter_items():
        update_age_gender_dist(channel_id)
        total_num += 1
        if total_num % batch_size == 0:
            logging.info('predicting age gender distribution... count: %d' % (total_num))
            time.sleep(1)
    logging.info('predict age gender distribution finish! total: %d' % (total_num))


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'update-age-gender-dist-from-pipe':
        update_age_gender_dist_from_pipe()
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
            'update-age-gender-dist-from-pipe', 
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

import argparse
import logging

from fans_dist import get_age_gender_distribution, dump_real_data, display_channel_contents
from fans_dist import predict_age_gender_dist_from_pipe
from age_gender_models import train as train_age_gender_model
from utils import watch_pipe, set_pipe, clean_pipe

def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'dump-real-data':
        dump_real_data()
    elif cmd == 'train-age-gender-model':
        train_age_gender_model()
    elif cmd == 'predict-age-gender-dist':
        channel_id = arguments['channel_id']
        get_age_gender_distribution(channel_id)
    elif cmd == 'predict-age-gender-dist-from-pipe':
        predict_age_gender_dist_from_pipe()
    elif cmd == 'display-channel-contents':
        channel_id = arguments['channel_id']
        display_channel_contents(channel_id)
    elif cmd == 'watch-pipe':
        watch_pipe()
    elif cmd == 'set-pipe':
        set_pipe()
    elif cmd == 'clean-pipe':
        clean_pipe()
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'dump-real-data',
            'train-age-gender-model',
            'predict-age-gender-dist', 
            'predict-age-gender-dist-from-pipe', 
            'display-channel-contents',
            'watch-pipe',
            'set-pipe',
            'clean-pipe',
        },
        required=True,
        help = 'run cmd type',
    ) 
    parser.add_argument(
        '--channel-id',
        help = 'channel id',
    ) 

    args = parser.parse_args()
    arguments = args.__dict__
    main(arguments)

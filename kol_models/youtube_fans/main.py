import argparse
import logging

from kol_models.youtube_fans.train_data import dump_train_data
from kol_models.youtube_fans.age_gender_inference import get_age_gender_distribution
from kol_models.youtube_fans.age_gender_inference import update_age_gender_dist


def display_channel_contents(channel_id):
    channel_contents = get_channel_contents_from_es(channel_id)
    for k, v in channel_contents.items():
        print(k, v)


def display_channel_data(channel_id):
    channel_data = ChannelData.From_es(channel_id)
    print(json.dumps(channel_data.get_data()))


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    channel_id = arguments.get('channel_id', None)
    if cmd == 'dump-train-data':
        dump_train_data()
    elif cmd == 'predict-age-gender-dist':
        #channel_id = arguments['channel_id']
        get_age_gender_distribution(channel_id)
    elif cmd == 'update-age-gender-dist':
        #channel_id = arguments['channel_id']
        update_age_gender_dist(channel_id)
    elif cmd == 'display-channel-contents':
        #channel_id = arguments['channel_id']
        display_channel_contents(channel_id)
    elif cmd == 'display-channel-data':
        #channel_id = arguments['channel_id']
        display_channel_data(channel_id)
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'dump-train-data',
            'predict-age-gender-dist', 
            'update-age-gender-dist', 
            'display-channel-contents',
            'display-channel-data',
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

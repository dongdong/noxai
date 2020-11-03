from redis_utils import set_pipe, clean_pipe, watch_pipe

import argparse
import logging

def update_channel(arguments):
    from update_tags import process_channel
    channel_id = arguments['channel_id']
    if channel_id:
        succ = process_channel(channel_id)
        logging.info('update channel finish. succ? ' + str(succ))
    else:
        logging.error('channel id required when update channel')


def clear_channel_tags(arguments):
    from update_tags import clear_channel_tags
    channel_id = arguments['channel_id']
    if channel_id:
        succ = clear_channel_tags(channel_id)
        logging.info('clear channel tags finish. succ? ' + str(succ))
    else:
        logging.error('channel id required when clear channel tags')
    

def update_channels_from_pipe():
    from update_tags import update_channel_tags_from_redis
    update_channel_tags_from_redis()


def stat_words_from_pipe():
    from update_tags import stat_feature_words_from_redis
    stat_feature_words_from_redis()


def main(arguments):
    print(arguments)
    cmd = arguments['cmd']
    if cmd == 'watch-pipe':
        watch_pipe()
    elif cmd == 'clean-pipe':
        clean_pipe()
    elif cmd == 'set-pipe':
        set_pipe()
    elif cmd == 'update-channel':
        update_channel(arguments)
    elif cmd == 'clear-channel-tags':
        clear_channel_tags(arguments)
    elif cmd == 'update-channels-from-pipe':
        update_channels_from_pipe()
    elif cmd == 'stat-words-from-pipe':
        stat_words_from_pipe()
    else:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmd',
        choices={
            'update-channel', 
            'clear-channel-tags', 
            'update-channels-from-pipe',
            'stat-words-from-pipe',
            'set-pipe',
            'clean-pipe',
            'watch-pipe',
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

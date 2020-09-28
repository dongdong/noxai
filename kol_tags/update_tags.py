from tag_inference import ChannelInfo, TagPredictorManager 
from es_utils import get_channel_contents, write_tag_info  
from redis_utils import redis_spider, redis_channel_id_list_name
import time

import logging

def process_channel(channel_id, channel_data=None):
    succ = False
    logging.info('process##channel, channel id: %s' % (channel_id))
    #time.sleep(0.1)
    #return True
    channel_info = ChannelInfo.Load(channel_id, channel_data)
    if channel_info:
        channel_info.process_tags()
        pre_tag_info_list = channel_info.channel_tag_detail
        tag_info_list = channel_info.channel_structure_tag_list
        if tag_info_list and len(tag_info_list) > 0:
            succ = write_tag_info(channel_id, tag_info_list)
            logging.info('Process channel. Update channel succ? %s, channel id: %s' % (str(succ), channel_id))
            logging.info('Process channel. Tags before: %s' % pre_tag_info_list)
            logging.info('Process channel. Tags after: %s' % tag_info_list)
            logging.info('Process channel. All predict channel tags: %s' % channel_info.channel_tag_score_list)
        else:
            logging.info('Process channel. No tags to update. channel id: %s' % channel_id)
    return succ


def get_channel_id_from_redis():
    while True:
        channel_id = redis_spider.lpop(redis_channel_id_list_name)
        if channel_id is None:
            break
        else:
            yield str(channel_id, encoding='utf-8')


def update_channel_tags():
    batch_size = 100
    total_num = 0
    succ_num = 0
    wait_time = 0
    max_wait_time = 180
    while True:
        channel_id_bytes = redis_spider.lpop(redis_channel_id_list_name)
        if channel_id_bytes is None:
            wait_time += 1
            if wait_time % 10 == 0:
                logging.info('Waiting for producer to generate channel id. time: %d' % wait_time)
            if wait_time > max_wait_time: 
                logging.info('wait time reach max, exit.')
                break
            else:
                time.sleep(1)
                #continue
        else: 
            wait_time = 0
            channel_id = str(channel_id_bytes, encoding='utf-8')
            succ = process_channel(channel_id)
            total_num += 1
            if succ:
                succ_num += 1
            if total_num % batch_size == 0:    
                logging.info('STATS. update channel tags, processed: %d, succ: %d.' % (total_num, succ_num))
    
    logging.info('STATS. update channel tags finish. total channels: %d, update channels: %d.' % (total_num, succ_num))
    #unknown_topic_id_set = TagPredictorManager.Get_unknown_topic_ids()
    #logging.info('Unknown topic ids: %s' % (unknown_topic_id_set))


def test_process_channel():
    #channel_id = 'UCO6SoJNF3VY2tnlzypHl-4w'
    #channel_id = 'UCLq6O9U5IJIOENn7WCaj6CA'   

    channel_id = 'UCNzsYU0aWwjERj-9Y9HUEng'
    #channel_id = 'UCuN4A3GCUq5-0wJDSiJoxRQ'
    #channel_id = 'UCvj8vNOUbLgPrBTuAqjew6A'
    #channel_id = 'UCCgLoMYIyP0U56dEhEL1wXQ'
    #channel_id = 'UCmRY4NSGK52lP_Lz11CjdYw'
    #channel_id = 'UCpB959t8iPrxQWj7G6n0ctQ'
    #channel_id = 'UCucot-Zp428OwkyRm2I7v2Q'
    #channel_id = 'UCgEHcR9LooxZ-rLTNJPMLkQ'

    channel_contents = get_channel_contents(channel_id)
    prev_tag_detail = channel_contents.get('tag_detail', None)
    succ = process_channel(channel_id, channel_contents)
    channel_contents = get_channel_contents(channel_id)
    after_tag_detail = channel_contents.get('tag_detail', None)
    print('success? ', succ)
    print('before: ', prev_tag_detail)
    print('after: ', after_tag_detail)

if __name__ == '__main__':
    #test_process_channel()
    update_channel_tags()



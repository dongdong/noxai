from tag_inference import ChannelInfo, TagPredictor 
from es_utils import get_channel_contents, write_tag_info  
from redis_utils import redis_spider, redis_channel_id_list_name
import time

import logging

def process_channel(channel_id, channel_data=None):
    succ = False
    logging.info('process##channel, channel id: %s' % (channel_id))

    channel_info = ChannelInfo.Load(channel_id, channel_data)
    if channel_info:
        channel_info.process_tags()
        pre_tag_info_list = channel_info.channel_tag_detail
        pre_all_tag_name_list = channel_info.channel_tag_list
        tag_info_list = channel_info.channel_structure_tag_list
        all_tag_name_list = channel_info.channel_all_tag_name_list
        succ = write_tag_info(channel_id, tag_info_list, all_tag_name_list)
        logging.info('Process channel. channel country: %s, language: %s' 
                % (channel_info.channel_country, channel_info.channel_language))
        #logging.info('Process channel. Channel video tags: %s' 
        #        % channel_info.get_channel_video_tags())
        logging.info('Process channel. Update channel succ? %s, channel id: %s' 
                % (str(succ), channel_id))
        logging.info('Structure tags before: %s' % pre_tag_info_list)
        logging.info('Structure tags after: %s' % tag_info_list)
        logging.info('Unstructure tags before: %s' % pre_all_tag_name_list)
        logging.info('Unstructure tags after: %s' % all_tag_name_list)

    return succ


def get_channel_id_from_redis():
    while True:
        channel_id = redis_spider.lpop(redis_channel_id_list_name)
        if channel_id is None:
            break
        else:
            yield str(channel_id, encoding='utf-8')


def update_channel_tags_from_redis():
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
                logging.info('Waiting for producer to generate channel id. time: %d' 
                        % wait_time)
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
                logging.info('STATS. update channel tags, processed: %d, succ: %d.' 
                        % (total_num, succ_num))
    
    logging.info(('STATS. update channel tags finish. total channels: %d, ' 
            + 'update channels: %d.') 
            % (total_num, succ_num))



def stat_feature_words(channel_id):
    succ = False
    channel_info = ChannelInfo.Load(channel_id)
    if channel_info:
        print(channel_id)
        channel_info.stat_feature_words()
        succ = True
    return succ


def dump_all_tags(channel_id):
    succ = False
    channel_info = ChannelInfo.Load(channel_id)
    if channel_info:
        #print('channel: ', channel_id)
        channel_info.stat_feature_words()
        channel_info.process_tags() 
        all_tag_name_list = channel_info.channel_all_tag_name_list
        print('model tags:', all_tag_name_list)
        succ = True
    return succ



def get_feature_words(channel_id):
    channel_info = ChannelInfo.Load(channel_id)
    if channel_info:
        feature_words = channel_info.get_feature_words()
        succ = True
    else:
        feature_words = None
        succ = False
    return succ, feature_words


def dump_feature_words_stats():
    word_size = 10000
    count_min_threshold = 5
    for feature_word, count in TagPredictor.feature_words_stats.most_common(word_size):
        if count > count_min_threshold:
            print(feature_word, count)


def stat_feature_words_from_redis():
    batch_size = 10
    total_num = 0
    succ_num = 0
    wait_time = 0
    max_wait_time = 60
    while True:
        channel_id_bytes = redis_spider.lpop(redis_channel_id_list_name)
        if channel_id_bytes is None:
            wait_time += 1
            if wait_time % 10 == 0:
                logging.info('Waiting for producer to generate channel id. time: %d' 
                        % wait_time)
            if wait_time > max_wait_time: 
                logging.info('wait time reach max, exit.')
                break
            else:
                time.sleep(1)
                #continue
        else: 
            wait_time = 0
            channel_id = str(channel_id_bytes, encoding='utf-8')

            succ = stat_feature_words(channel_id)
            #succ, feature_word_list = get_feature_words(channel_id)
            #succ = dump_all_tags(channel_id)

            total_num += 1
            if succ:
                succ_num += 1
            if total_num % batch_size == 0:    
                logging.info('STATS. stat channel keywords, processed: %d, succ: %d.' 
                        % (total_num, succ_num))
                time.sleep(1)
    
    logging.info('stat feature word finish. total channels: %d, succ: %d.' 
            % (total_num, succ_num))

    dump_feature_words_stats()


def clear_channel_tags(channel_id):
    channel_contents = get_channel_contents(channel_id)
    tag_detail = channel_contents.get('tag_detail', [])
    tag_list = channel_contents.get('tag_list', [])
    logging.info('clear channel tags. prev tag detail: %s, prev tag list: %s' 
            % (tag_detail, tag_list))
    succ = write_tag_info(channel_id, [], [])
    channel_contents = get_channel_contents(channel_id)
    tag_detail = channel_contents.get('tag_detail', [])
    tag_list = channel_contents.get('tag_list', [])
    logging.info('clear channel tags. after tag detail: %s, after tag list: %s' 
            % (tag_detail, tag_list))
    return succ
    

def test_clear_channel_tags():
    #channel_id = 'UCi7uAKezhxT0q7y4_Z6SgIA'
    #channel_id = 'UC3l6ZCIGr-7T1ALIMMAnyTg'
    channel_id = 'UCk-Dau8JTXg6slA-0X0ye4w'
    clear_channel_tags(channel_id)
    

def test_process_channel():
    #channel_id = 'UC6E2mP01ZLH_kbAyeazCNdg'
    channel_id = 'UCQ3i4wFECsKSLp8_9y-BkgA'
    #channel_id = ''

    channel_contents = get_channel_contents(channel_id)
    prev_tag_detail = channel_contents.get('tag_detail', None)
    succ = process_channel(channel_id, channel_contents)
    channel_contents = get_channel_contents(channel_id)
    after_tag_detail = channel_contents.get('tag_detail', None)
    #print('success? ', succ)
    #print('before: ', prev_tag_detail)
    #print('after: ', after_tag_detail)


def test_stat_feature_words():
    channel_id_list = [
        'UCi7uAKezhxT0q7y4_Z6SgIA',
        'UC3l6ZCIGr-7T1ALIMMAnyTg',
        'UCk-Dau8JTXg6slA-0X0ye4w',
        'UC6E2mP01ZLH_kbAyeazCNdg',
        'UCQ3i4wFECsKSLp8_9y-BkgA',
    ]
    for channel_id in channel_id_list:
        stat_feature_words(channel_id)
    dump_feature_words_stats()


def test_get_feature_words():
    channel_id_list = [
        'UCi7uAKezhxT0q7y4_Z6SgIA',
        'UC3l6ZCIGr-7T1ALIMMAnyTg',
        'UCk-Dau8JTXg6slA-0X0ye4w',
        'UC6E2mP01ZLH_kbAyeazCNdg',
        'UCQ3i4wFECsKSLp8_9y-BkgA',
    ]
    for channel_id in channel_id_list:
        succ, feature_word_list = get_feature_words(channel_id)


def test_dump_all_tags():
    #channel_id = 'UCQ3i4wFECsKSLp8_9y-BkgA'
    channel_id = 'UCiBr0bK06imaMbLc8sAEz0A'
    dump_all_tags(channel_id)    


if __name__ == '__main__':
    #clear_channel_tags()
    #test_process_channel()
    #update_channel_tags_from_redis()
    #test_stat_feature_words()    
    #test_get_feature_words()    
    test_dump_all_tags()



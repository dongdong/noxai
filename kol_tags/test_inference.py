#from tag_inference import ChannelInfo, VideoTags, TagPredictor, TagPredictorManager 
#from es_utils import get_channel_list_by_category_language
import os
import json

def get_channel_data():
    category_list = [24, 25, 26]
    language_list = ['en', 'zh-Hans', 'zh-Hant']
    size = 10

    for language in language_list:
        for category in category_list:
            channel_list = get_channel_list_by_category_language(category, language, size)
            for channel_data in channel_list:
                yield channel_data


def dump_channel_info(channel_info):
    print('*' * 20)
    for k, v in channel_info.get_data().items():
        if k == 'videos':
            for video in v:
                print('-' * 20)
                for k_1, v_1 in video.items():
                    if k_1 == 'description':
                        v_1 = '\n'.join(v_1.split('\n')[:3])
                    print(k_1, ':', v_1)
        else:
            print(k, ":", v)
 

def dump_unknown_topic_ids():
    unknown_topic_id_set = set()
    for language, tag_predictor in TagPredictorManager.tag_predictor_map.items():
        unknown_topic_id_set.update(tag_predictor.tag_index.unknown_topic_id_set)
    print('unknown topic ids: ', unknown_topic_id_set)


def test():
    for channel_data in get_channel_data():
        #print(channel_data)
        channel_id = channel_data['id']
        channel_info = ChannelInfo.Load(channel_id, channel_data)
        if channel_info:
            channel_info.process_tags()
            dump_channel_info(channel_info)
        else:
            print('Load channel info error!', channel_id)
    
    dump_unknown_topic_ids()


def dump_cache_file(data_path):
    if os.path.exists(data_path):
        with open(data_path) as f:
            for line in f:
                video_contents = json.loads(line)
                for k, v in video_contents.items():
                    print(k, v) 


if __name__ == '__main__':
    #test()
    #data_path = 'cache/zh-Hant/video_data/數碼產品_智能手機.json'
    #data_path = 'cache/zh-Hant/video_data_processed/數碼產品_智能手機.json'
    data_path = './cache/ja/video_data_processed/_.json'
    dump_cache_file(data_path)





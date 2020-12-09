from es_utils import get_pop_videos, get_video_contents
from video import VideoData
from collections import Counter, defaultdict
import traceback
import logging
import math

class PopVideoData(VideoData):
    def __init__(self, video_id, title, category_id, keyword_list, 
            description, language, pop_score):
        super(PopVideoData, self).__init__(video_id, title, category_id, 
                keyword_list, description, language)
        self.pop_score = pop_score 
    
    def get_data(self):
        data = super(PopVideoData, self).get_data()
        data['pop_score'] = self.pop_score
        return data

    @staticmethod
    def From_dict(json_obj):
        try:
            video_id = json_obj['id']
            title = json_obj['title']
            category_id = json_obj['category']
            keyword_list = json_obj.get('keywords', [])
            description = json_obj.get('description', '')
            language = json_obj['language']
            score = json_obj['score']
            pop_video_data = PopVideoData(video_id, title, category_id, keyword_list, 
                description, language, score) 
        except:
            logging.warn('Fail to load processed video data from dict, invalid contents %s' 
                    % (json_obj))
            pop_video_data = None
        return pop_video_data


def get_pop_video_data(pop_video):
    video_data = None
    try:
        #print(pop_video)
        video_id = pop_video['_id']
        video_description = '' #pop_video['_source']['description']
        video_contents = get_video_contents(video_id, video_description)
        if video_contents is not None:
            score = pop_video['sort'][0]
            score = round(math.log(score + 1), 2)
            video_contents['score'] = score
            video_data = PopVideoData.From_dict(video_contents)
            #print(video_data.get_data())
    except:
        logging.warn('Fail to create video data from contents: %s' % video_contents)
        logging.error(traceback.format_exc())
    return video_data


def get_unit_pop_video_data_list(language, country, size):
    video_data_list = []
    pop_video_search_list = get_pop_videos(language, country, size)
    for pop_video in pop_video_search_list:
        video_data = get_pop_video_data(pop_video)
        if video_data is not None:
            video_data_list.append(video_data)
    return video_data_list 


def get_pop_video_data_list(language_list, country_list, unit_size):
    logging.info(('get pop video. languages: %s, countries: %s, '
            + 'unit_size: %d') % (language_list, country_list, unit_size))
    video_data_list = []
    for language in language_list:
        for country in country_list:
            unit_list = get_unit_pop_video_data_list(language, country, unit_size)
            video_data_list += unit_list
            logging.info('get pop video. language: %s, country: %s, size: %d' 
                    % (language, country, len(unit_list)))
    logging.info('get pop video finish. total size: %d' % len(video_data_list))
    return video_data_list        


def get_video_feature_words(video_data):
    word_list = video_data.keyword_list
    return word_list


def get_pop_words(pop_video_data_list, size=32):
    score_dict = defaultdict(float)
    for pop_video_data in pop_video_data_list:
        pop_word_list = get_video_feature_words(pop_video_data)
        score = pop_video_data.pop_score
        for pop_word in pop_word_list:
            score_dict[pop_word] += score        
    pop_word_list = sorted(score_dict.items(), key=lambda x: x[1], 
            reverse=True)[:size]
    return pop_word_list


 
def test():
    #language_list = ['zh']
    #language_list = ['zh-Hant']
    language_list = ['en']
    #language_list = ['zh', 'zh-Hant']
    country_list = [None]
    unit_size = 2048
    
    video_data_list = get_pop_video_data_list(language_list, country_list, unit_size)
    #pop_words = get_pop_words(video_data_list)
    #print(pop_words)    
    #category_counter = defaultdict(int)
    category_videos_dict = {}
    for video_data in video_data_list:
        category = video_data.category_id
        if category not in category_videos_dict:
            category_videos_dict[category] = []
        category_videos_dict[category].append(video_data)

    for category, category_video_list in category_videos_dict.items():
        size = len(category_video_list)
        print(category, size)
        if size > 8:
            pop_words = get_pop_words(category_video_list)
            print(pop_words)


if __name__ == '__main__':
    test()

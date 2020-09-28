import path_manager as pm
from tag_model import TagModel, TagMatchModel
from tag import TagIndex
from video import VideoData, VideoInfo
from text_processor import TFIDFModel
from es_utils import get_channel_contents, get_channel_video_list, get_video_description_batch
from data_process import get_feature_data_from_video_info_iter
from youtube_api import get_youtube_video_topics_batch

from collections import Counter, defaultdict
import logging
from datetime import datetime
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

now = datetime.now()

CHANNEL_VIDEO_COUNT_MAX = 32
CHANNEL_VIDEO_COUNT_MIN = 5
CHANNEL_TAG_TOTAL_MAX = 5
CHANNEL_TAG_COUNT_THRESHOLD = 6
CHANNEL_UPDATE_DAYS_THRESHOLD = 180
CHANNEL_SUB_NUM_MIN_THRESHOLD = 10000

def _get_video_info_list(channel_id):
    max_video_size = CHANNEL_VIDEO_COUNT_MAX
    video_info_list = []
    video_contents_list = get_channel_video_list(channel_id, max_video_size)
    video_id_list = [video_contents['id'] for video_contents in video_contents_list]
    video_id_description_map = get_video_description_batch(video_id_list)
    for video_contents in video_contents_list:
        video_id = video_contents['id']
        #video_contents['description'] = get_video_description(video_id)
        video_contents['description'] = video_id_description_map.get(video_id, '')
        video_data = VideoData.From_dict(video_contents)
        video_info = VideoInfo(video_data)
        video_info_list.append(video_info)
    return video_info_list


def _load_models(language):
    tfidf_model_path = pm.get_tfidf_inference_model_dir(language)
    tag_model_path = pm.get_tag_inference_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_path)
    tfidf_model.load_model()
    tag_model = TagModel()
    tag_model.load_model(tag_model_path)
    tag_index = TagIndex.Create(language)
    return tfidf_model, tag_model, tag_index


def _predict(video_info_list, tfidf_model, tag_model):
    feature_data = get_feature_data_from_video_info_iter(video_info_list, tfidf_model)
    ret = tag_model.get_top_prob_class_list_batch(feature_data.X)
    return ret


def normalize_score(score):
    score = score / 10
    if score > 1:
        score = 1
    return score

def _get_sorted_tag_score_list(pred_tag_result, video_data, tag_index, tag_model, tag_match_model, topic_id_list, max_size=3):
    tag_1_class_prob_list, tag_2_class_prob_list = tag_model.get_valid_tag_list(pred_tag_result) 
    addition_tag_class_list = tag_match_model.get_tag_class_list(video_data.get_feature_words())
    tag_class_score_map = defaultdict(int)
    for tag_class, tag_prob in tag_1_class_prob_list + tag_2_class_prob_list:
        tag_class_score_map[tag_class] += tag_prob
    for tag_class in addition_tag_class_list:
        tag_class_score_map[tag_class] += 5
    for topic_id in topic_id_list:
        tag_class = tag_index.get_tag_name_by_topic_id(topic_id)
        if tag_class is not None:
            tag_class_score_map[tag_class] += 2
    tag_class_score_list = sorted([(tag_class, normalize_score(score)) for tag_class, score in tag_class_score_map.items()], 
            key=lambda x: x[1], reverse=True)
    return tag_class_score_list[:max_size]


class VideoTags():
    def __init__(self):
        self.topic_id_list = None
        #self.topic_category_list = None
        self.tag_score_list = None
    def process(self, video_data, pred_tag_result, topic_id_list, tag_index, tag_model, tag_match_model):
        #video_id = video_data.video_id
        #video_topics = get_youtube_video_topics(video_id)    
        #self.topic_id_list = video_topics['topic_ids']
        #self.topic_category_list = video_topics['topic_categories']
        self.topic_id_list = topic_id_list
        self.tag_score_list = _get_sorted_tag_score_list(pred_tag_result, video_data, 
                tag_index, tag_model, tag_match_model, topic_id_list)

    def get_data(self):
        data = {
            'topic_ids': self.topic_id_list,
            #'topic_categories': self.topic_category_list,
            'predict_tag_lists': self.tag_score_list,
        }
        return data


class TagPredictor():
    def __init__(self, language):
        self.language = language
        self.tfidf_model, self.tag_model, self.tag_index = _load_models(language)
        self.tag_match_model = TagMatchModel(language)

    def get_channel_structure_tag_list(self, channel_tag_list):
        language_map = {
            'zh-Hant': 'zh',
            'zh-Hans': 'zh',
        }
        channel_structure_tag_list = []
        for tag_name, score in channel_tag_list:
            structure_info = self.tag_index.get_structure_tag_info(tag_name)
            if structure_info:
                if self.language in language_map:
                    language = language_map[self.language]
                else:
                    language = self.language
                name = structure_info[language]
                if name is None:
                    name = tag_name
                data = {
                    'level': structure_info['level'], 
                    'tag_name': name, 
                    'weight': score,
                    'language': language,
                    'id': structure_info['id'],
                } 
                channel_structure_tag_list.append(data) 
        return channel_structure_tag_list

    def get_channel_video_tags(self, video_info_list):
        video_tags_map = {}
        channel_tag_counter = Counter()
        video_id_list = [video_info.video_data.video_id for video_info in video_info_list]
        video_id_topic_list_map = get_youtube_video_topics_batch(video_id_list) 
        pred_tag_list = _predict(video_info_list, self.tfidf_model, self.tag_model)
        for video_info, pred_tag_result in zip(video_info_list, pred_tag_list):
            video_tags = VideoTags()
            video_data = video_info.video_data
            topic_id_list = video_id_topic_list_map.get(video_data.video_id, [])
            video_tags.process(video_data, pred_tag_result, topic_id_list, self.tag_index, 
                    self.tag_model, self.tag_match_model)
            video_tags_map[video_data.video_id] = video_tags

            tag_name_list = [tag_name for tag_name, tag_score in video_tags.tag_score_list
                    if tag_score > 0.1]
            channel_tag_counter.update(tag_name_list)   

        total_count = len(video_info_list)
        channel_tag_list = [ (tag, count / total_count) 
                for tag, count in channel_tag_counter.most_common(CHANNEL_TAG_TOTAL_MAX) 
                if count >= CHANNEL_TAG_COUNT_THRESHOLD]

        return channel_tag_list, video_tags_map
    

class TagPredictorManager():
    languages = ['en', 'zh-Hans', 'zh-Hant']
    tag_predictor_map = {}
    for language in languages:
        tag_predictor_map[language] = TagPredictor(language)
    tag_predictor_map['zh'] = tag_predictor_map['zh-Hant']
    default_predictor = tag_predictor_map['en']
    @classmethod
    def Get_predictor(cls, language):
        if language in cls.tag_predictor_map:
            tag_predictor = cls.tag_predictor_map[language]
        else:
            logging.warn('Unknown language for tag predict: %s' % language)
            tag_predictor = cls.default_predictor
        return tag_predictor
    @classmethod
    def Get_unknown_topic_ids(cls):
        unknown_topic_id_set = set()
        for tag_predictor in cls.tag_predictor_map.values():
            id_set = tag_predictor.tag_index.unknown_topic_id_set
            unknown_topic_id_set.update(id_set)
        return unknown_topic_id_set


def _is_valid_channel_to_update_tags(channel_id, channel_data): 

    required_names = ['sub_num', 'languages', 'latest_three_pub_date']
    for name in required_names:
        if name not in channel_data:
            logging.info('information miss: ' % name)
            return False

    sub_num = channel_data['sub_num']    
    if sub_num < CHANNEL_SUB_NUM_MIN_THRESHOLD:
        logging.info('Too few subscribes, ignore! channel: %s, sub num: %d' % (channel_id, sub_num))
        return False
    
    support_language_set = TagPredictorManager.tag_predictor_map.keys()
    language = channel_data['languages']
    if language not in support_language_set:
        logging.info('Language not support yet. channel: %s, language: %s' % (channel_id, language))
        return False

    latest_video_timestamp = channel_data['latest_three_pub_date']
    latest_video_date = datetime.fromtimestamp(latest_video_timestamp / 1000)
    delta = now - latest_video_date
    if delta.days > CHANNEL_UPDATE_DAYS_THRESHOLD:      
        logging.info('Channel have not updated for a while! channel: %s, days: %d' % (channel_id, delta.days))
        return False

    return True

def _get_channel_info(channel_id, channel_data, channel_video_list):
        if not channel_data:
            channel_data = get_channel_contents(channel_id)
            if channel_data is None or not _is_valid_channel_to_update_tags(channel_id, channel_data):
                logging.info('Channel is not valid to update tags, channel: %s' % (channel_id))
                return None
        if channel_data is None:
            return None
        if not channel_video_list:
            channel_video_list = _get_video_info_list(channel_id) 
        if channel_video_list is not None and len(channel_video_list) > CHANNEL_VIDEO_COUNT_MIN:
            channel_info = ChannelInfo(channel_id, channel_data, channel_video_list)
        else:
            logging.warn('Fail to load channel videos. channel id: %s, channel videos: %d' % 
                        (channel_id, len(channel_video_list)))
            channel_info = None
        return channel_info


class ChannelInfo():
    def __init__(self, channel_id, channel_data, video_info_list):
        self.channel_id = channel_id
        self.channel_title = channel_data.get('title', '')
        self.channel_country = channel_data.get('country', '')
        self.channel_language = channel_data.get('languages', '')
        self.channel_tag_detail = channel_data.get('tag_detail', [])
        self.channel_data = channel_data
        self.video_info_list = video_info_list
        self.tag_predictor = TagPredictorManager.Get_predictor(self.channel_language)
        self.channel_tag_score_list = None
        self.channel_video_tag_dict = None
        self.channel_structure_tag_list = None

    def process_tags(self):
        self.channel_tag_score_list, self.channel_video_tag_dict = self.tag_predictor.get_channel_video_tags(
                self.video_info_list)
        self.channel_structure_tag_list = self.tag_predictor.get_channel_structure_tag_list(
                self.channel_tag_score_list)

    def get_data(self):
        data = {
            'channel_id': self.channel_id,
            'channel_title': self.channel_title,
            'channel_country': self.channel_country,
            'channel_language': self.channel_language,
        }
        if self.channel_tag_score_list:
            data['channel_tags'] = self.channel_tag_score_list
        if self.channel_structure_tag_list:
            data['channel_structure_tag_list'] = self.channel_structure_tag_list
        video_data_list = []
        for video_info in self.video_info_list:
            video_data = video_info.video_data
            video_data_dict = video_data.get_data()
            video_id = video_data.video_id
            if self.channel_video_tag_dict and video_id in self.channel_video_tag_dict:
                video_tags = self.channel_video_tag_dict[video_id]
                video_data_dict.update(video_tags.get_data())
            video_data_list.append(video_data_dict)
        data['videos'] = video_data_list
        return data
    
    @staticmethod
    def Load(channel_id, channel_data=None, channel_video_list=None):
        channel_info = None
        try:
            channel_info = _get_channel_info(channel_id, channel_data, channel_video_list)
        except Exception as e:
            pass
            #logging.warn('Exception occured when load channel. channel id: %s' % channel_id)
            print(repr(e))
        return channel_info



def test():
    channel_id_list = [
        'UCCoCaPU4z9zlACXP5Kc24ow',
        'UCgMBZ9epGac-dKeoZ4vsX8A',
        'UCO6SoJNF3VY2tnlzypHl-4w',
    ] 
    for channel_id in channel_id_list:
        channel_info = ChannelInfo.Load(channel_id)
        channel_info.process_tags()
        if channel_info:
            print('*' * 20)
            for k, v in channel_info.get_data().items():
                if k == 'videos':
                    for video in v:
                        print('-' * 20)
                        for k_1, v_1 in video.items():
                            if k_1 not in ['description', ]:
                                print(k_1, ':', v_1)
                else:
                    print(k, ":", v)
        else:
            print('Load channel info error!', channel_id)
                
    

if __name__ == '__main__':
    test()



      

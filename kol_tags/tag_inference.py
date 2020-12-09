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

logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

now = datetime.now()

CHANNEL_VIDEO_COUNT_MAX = 32
CHANNEL_VIDEO_COUNT_MIN = 8
VIDEO_TAG_TOTAL_MAX = 5 #3 #5
CHANNEL_TAG_TOTAL_MAX = 5 #3 #5
CHANNEL_TAG_COUNT_THRESHOLD = 4 #5 #6
CHANNEL_UPDATE_DAYS_THRESHOLD = 365 * 3
CHANNEL_SUB_NUM_MIN_THRESHOLD = 10000
MATCH_MODEL_TAG_SCORE = 3
YOUTUBE_TOPIC_TAG_SCORE = 2
MODEL_SCORE_MAX = 10

def _get_video_info_list(channel_id, channel_data):
    video_info_list = []
    total_videos = channel_data.get('total_videos', 0)
    if total_videos < CHANNEL_VIDEO_COUNT_MIN:
        logging.warn('channel do not have enough videos. video count: %d' % total_videos)
        return video_info_list
    max_video_size = CHANNEL_VIDEO_COUNT_MAX
    video_contents_list = get_channel_video_list(channel_id, max_video_size)
    if len(video_contents_list) < CHANNEL_VIDEO_COUNT_MIN:
        logging.warn('channel do not have enough valid videos. video count: %d, valid: %d' 
                % (total_videos, len(video_contents_list)))
        return video_info_list
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


def _predict(video_info_list, tfidf_model, tag_model):
    video_id_pred_item_map = {}
    feature_data, valid_video_id_list = get_feature_data_from_video_info_iter(
            video_info_list, tfidf_model)
    logging.info('video tag predict. total video: %d, valid video: %d' 
            % (len(video_info_list), len(valid_video_id_list)))
    if len(valid_video_id_list) > 0:
        pred_result = tag_model.get_top_prob_class_list_batch(feature_data.X)
        video_id_pred_item_map = {video_id:pred_item 
                for video_id, pred_item in zip(valid_video_id_list, pred_result)}
    ret = []
    for video_info in video_info_list:
        video_id = video_info.video_data.video_id
        #if video_id in video_id_pred_item_map:
        pred_item = video_id_pred_item_map.get(video_id, None)
        ret.append(pred_item)
    return ret


def normalize_score(tag_score_list):
    total_score = 0
    for tag, score in tag_score_list:
        total_score += score
    if total_score > 0:
        tag_score_list = [(tag, score/total_score) for tag, score in tag_score_list]
    return tag_score_list


def _load_models(language):
    tfidf_model_path = pm.get_tfidf_inference_model_dir(language)
    tag_model_path = pm.get_tag_inference_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_path)
    tfidf_model.load_model()
    tag_model = TagModel()
    tag_model.load_model(tag_model_path)
    tag_match_model = TagMatchModel(language)
    return tfidf_model, tag_model, tag_match_model


def _load_feature_word_dict():
    feature_word_dict = {}
    path = pm.get_feature_word_dict_path()
    with open(path, 'r') as f:
        for line in f:
            words = line.strip().split(', ')
            if len(words) > 0:
                value = words[0]
                for word in words:
                    feature_word_dict[word] = value
    return feature_word_dict


class TagPredictor():
    languages = ['en', 'zh-Hans', 'zh-Hant', 'ko']
    models_map = {}
    tag_index_map = {}
    for language in languages:
        models_map[language] = _load_models(language)
        tag_index_map[language] = TagIndex.Create(language)
    models_map['zh'] = models_map['zh-Hant']
    #models_map['zh'] = models_map['zh-Hans']
    default_models = models_map['en']
    tag_index_map['zh'] = tag_index_map['zh-Hant']
    #tag_index_map['zh'] = tag_index_map['zh-Hans']
    default_tag_index = tag_index_map['en']

    feature_word_dict = _load_feature_word_dict()
    feature_words_stats = Counter()

    def __init__(self, language, video_info_list):
        self.language = language
        if language in TagPredictor.models_map:
            models = TagPredictor.models_map[language]
            self.tfidf_model, self.tag_model, self.tag_match_model = models 
        else:
            default_models = TagPredictor.default_models
            self.tfidf_model, self.tag_model, self.tag_match_model = default_models
            self.tag_model = None

        if language in TagPredictor.tag_index_map:
            self.tag_index = TagPredictor.tag_index_map[language]
        else:
            self.tag_index = TagPredictor.default_tag_index

        self.video_info_list = video_info_list
        self.video_id_list = [video_info.video_data.video_id 
            for video_info in video_info_list]
        self.total_videos = len(video_info_list)
        self.video_topic_list_map = None
        self.video_model_tag_score_map = None
        self.video_match_model_tag_list_map = None
        

    def _process_tags(self):
        if not self.video_topic_list_map:
            self._get_ytb_topic_tag_list_map()
        if not self.video_model_tag_score_map:
            self._get_model_tag_score_map()
        if not self.video_match_model_tag_list_map:
            self._get_match_model_tag_list_map()

    def _get_ytb_topic_tag_list_map(self):
        self.video_topic_list_map = {}
        if self.total_videos < CHANNEL_VIDEO_COUNT_MIN:
            return
        self.video_topic_list_map = get_youtube_video_topics_batch(self.video_id_list) 
        #logging.info('youtube topic tags: %s' % str(self.video_topic_list_map))

    def _get_model_tag_score_map(self):
        self.video_model_tag_score_map = {}
        if self.total_videos < CHANNEL_VIDEO_COUNT_MIN:
            return
        if self.tag_model:
            pred_tag_list = _predict(self.video_info_list, self.tfidf_model, 
                    self.tag_model)
            for video_info, pred_tag_result in zip(self.video_info_list, pred_tag_list):
                video_id = video_info.video_data.video_id
                ret = self.tag_model.get_valid_tag_list(pred_tag_result) 
                tag_1_class_prob_list, tag_2_class_prob_list = ret 
                tag_class_score_map = defaultdict(int)
                for tag_class, tag_prob in tag_1_class_prob_list + tag_2_class_prob_list:
                    tag_class_score_map[tag_class] += tag_prob
                self.video_model_tag_score_map[video_id] = tag_class_score_map
        #logging.info('model tags: %s' % str(self.video_model_tag_score_map))


    def _get_match_model_tag_list_map(self):
        self.video_match_model_tag_list_map = {}
        if self.total_videos < CHANNEL_VIDEO_COUNT_MIN:
            return
        if self.tag_match_model:
            for video_info in self.video_info_list:
                video_data = video_info.video_data
                video_id = video_data.video_id
                match_model_tags = self.tag_match_model.get_tag_class_list(
                        video_data.get_feature_words())
                self.video_match_model_tag_list_map[video_id] = match_model_tags
        #logging.info('match model tags: %s' % str(self.video_match_model_tag_list_map))
    
    def get_video_tag_score_list(self, video_id):
        topic_id_list = self.video_topic_list_map.get(video_id, [])
        model_tag_score_map = self.video_model_tag_score_map.get(video_id, {})
        match_model_tag_list = self.video_match_model_tag_list_map.get(video_id, [])
        
        tag_class_score_map = defaultdict(int)
        for tag_class, score in model_tag_score_map.items():
            if score > MODEL_SCORE_MAX:
                score = MODEL_SCORE_MAX
            tag_class_score_map[tag_class] += score
        for tag_class in match_model_tag_list:
            tag_class_score_map[tag_class] += MATCH_MODEL_TAG_SCORE
        for topic_id in topic_id_list:
            tag_class = self.tag_index.get_tag_name_by_topic_id(topic_id)
            if tag_class is not None:
                tag_class_score_map[tag_class] += YOUTUBE_TOPIC_TAG_SCORE

        sorted_tag_class_score_list = sorted(
                [(tag_class, score) 
                        for tag_class, score in tag_class_score_map.items()
                        if tag_class != ''], 
                key=lambda x: x[1], reverse=True)[:VIDEO_TAG_TOTAL_MAX]
        logging.info('video tags: %s' % str(sorted_tag_class_score_list))
        return sorted_tag_class_score_list

    def get_tags(self):
        channel_tag_list = []
        video_tags_map = {}
        channel_tag_counter = Counter()
        channel_tag_score = defaultdict(float)
        
        self._process_tags()
        for video_id in self.video_id_list:
            tag_score_list = self.get_video_tag_score_list(video_id)
            video_tags_map[video_id] = tag_score_list
            tag_name_list = []
            for tag_name, tag_score in tag_score_list:
                channel_tag_score[tag_name] += tag_score
                tag_name_list.append(tag_name)
            channel_tag_counter.update(tag_name_list)   
        
        channel_tag_list = sorted(
                [(tag, score) for tag, score in channel_tag_score.items()
                        if channel_tag_counter[tag] >= CHANNEL_TAG_COUNT_THRESHOLD],
                key=lambda x: x[1],
                reverse=True)[:CHANNEL_TAG_TOTAL_MAX]
        channel_tag_list = normalize_score(channel_tag_list)
        logging.info('tag list: %s' % channel_tag_list)
        total_count = len(self.video_id_list)
        channel_tag_list = [(tag, score * channel_tag_counter[tag] / total_count) 
                for tag, score in channel_tag_list
                if score > 0.1]
        return channel_tag_list, video_tags_map

    def get_structure_tag_list(self, channel_tag_list):
        language_map = {
            'zh-Hant': 'zh',
            'zh-Hans': 'zh',
            'zh': 'zh',
        }
        default_language = 'en'
        if self.language in language_map:
            language = language_map[self.language]
        elif self.language in TagPredictor.languages:
            language = self.language
        else:
            logging.warn('Warn: language %s not support yet.' % self.language)
            language = default_language
        channel_structure_tag_map = {}

        for tag_name, score in channel_tag_list:
            structure_info = self.tag_index.get_structure_tag_info(tag_name)
            if structure_info:
                #print(structure_info)
                #name = structure_info[language]
                #if name is None or name == '':
                #    name = tag_name
                tag_id = structure_info['id']
                tag_level = structure_info['level']
                if tag_id not in channel_structure_tag_map: 
                    data = {
                        'level': tag_level, 
                        'tag_name': tag_name, 
                        'weight': score,
                        'language': language,
                        'id': tag_id,
                    } 
                    channel_structure_tag_map[tag_id] = data 
                else:
                    channel_structure_tag_map[tag_id]['weight'] += score
        channel_structure_tag_list = list(channel_structure_tag_map.values())
        return channel_structure_tag_list

    def stat_feature_words(self, size=8, count_min_threshold=3):
        feature_word_counter = self._get_feature_word_counter()
        feature_words = [word for word, count 
            in feature_word_counter.most_common(size)
            if count > count_min_threshold]
        #print('feature words：', feature_words)
        TagPredictor.feature_words_stats.update(feature_words)

        feature_word_stats = [(word, count) for word, count 
            in feature_word_counter.most_common(size)
            if count > count_min_threshold]
        print('feature words：', feature_word_stats)

    def get_feature_words(self, size=8, count_min_threshold=3):
        feature_word_counter = self._get_feature_word_counter()
        feature_word_set = set()
        not_feature_word_set = set()
        for word, count in feature_word_counter.most_common(size):
            if count <= count_min_threshold:
                continue
            if word in TagPredictor.feature_word_dict:
                feature_word = TagPredictor.feature_word_dict[word]
                feature_word_set.add(feature_word)
            else:
                not_feature_word_set.add(word)
        logging.info('feature words: %s' % str(feature_word_set))
        logging.info('not feature words: %s' % str(not_feature_word_set))
        return list(feature_word_set)

    def _get_video_feature_words(self, video_data):
        #video_data.process_tfidf_words(self.tfidf_model)
        #feature_word_list = [word for word, score in video_data.tfidf_word_list]
        feature_word_list = [word.lower() for word in video_data.keyword_list]
        #feature_word_set = set()
        #for word in video_data.keyword_list:
        return feature_word_list

    def _get_feature_word_counter(self):
        feature_word_counter = Counter()
        for video_info in self.video_info_list:
            video_data = video_info.video_data
            feature_word_list = self._get_video_feature_words(video_data)
            feature_word_counter.update(feature_word_list)
            #print(video_data.video_id, feature_word_list)
        return feature_word_counter


def _is_valid_channel_to_update_tags(channel_id, channel_data): 
    required_names = ['is_delete']
    for name in required_names:
        if name not in channel_data:
            logging.info('information miss: %s' % name)
            return False

    is_delete = (channel_data['is_delete'] != 1)
    if is_delete:
        logging.info('Channel has been deleted! channel id: %s' % (channel_id))
        return False

    ''' 
    total_videos = channel_data.get('total_videos', 0)
    if total_videos < CHANNEL_VIDEO_COUNT_MIN: 
        logging.info('Channel do not have enough videos! channel id: %s, videos: %d' 
                % (channel_id, total_videos))
        return False
 
    sub_num = channel_data['sub_num']    
    if sub_num < CHANNEL_SUB_NUM_MIN_THRESHOLD:
        logging.info('Too few subscribes, ignore! channel id: %s, sub num: %d' 
                % (channel_id, sub_num))
        return False

    support_language_set = TagPredictor.models_map.keys()
    language = channel_data.get('languages', '')
    if language not in support_language_set:
        logging.info('Language not support yet. channel id: %s, language: %s' 
                % (channel_id, language))
        return False

    latest_video_timestamp = channel_data.get('latest_three_pub_date', 0)
    latest_video_date = datetime.fromtimestamp(latest_video_timestamp / 1000)
    delta = now - latest_video_date
    if delta.days > CHANNEL_UPDATE_DAYS_THRESHOLD:      
        logging.info('Channel have not updated for a while! channel id: %s, days: %d' 
                % (channel_id, delta.days))
        return False
    '''

    return True


def _get_channel_info(channel_id, channel_data, channel_video_list):
        if not channel_data:
            channel_data = get_channel_contents(channel_id)
            if channel_data is None or not _is_valid_channel_to_update_tags(
                    channel_id, channel_data):
                logging.info('Channel not valid to update tags, channel id: %s' % (channel_id))
                return None
        if channel_data is None:
            return None

        if not channel_video_list: 
            channel_video_list = _get_video_info_list(channel_id, channel_data) 

        channel_info = ChannelInfo(channel_id, channel_data, channel_video_list)
        return channel_info


class ChannelInfo():
    def __init__(self, channel_id, channel_data, video_info_list):
        self.channel_id = channel_id
        self.channel_title = channel_data.get('title', '')
        self.channel_country = channel_data.get('country', '')
        self.channel_language = channel_data.get('languages', '')
        self.channel_tag_detail = channel_data.get('tag_detail', [])
        self.channel_tag_list = channel_data.get('tag_list', [])
        self.channel_data = channel_data
        self.video_info_list = video_info_list
        self.tag_predictor = TagPredictor(self.channel_language, video_info_list)
        self.channel_tag_score_list = None
        self.channel_video_tag_dict = None
        self.channel_structure_tag_list = None
        self.channel_all_tag_name_list = None

    def process_tags(self):
        self.channel_tag_score_list, self.channel_video_tag_dict = self.tag_predictor.get_tags()
        self.channel_structure_tag_list = self.tag_predictor.get_structure_tag_list(
                self.channel_tag_score_list)
        
        #self.channel_all_tag_name_list = [tag_name 
        #        for tag_name, score in self.channel_tag_score_list]
        tag_name_list = [tag_name.lower() 
                for tag_name, score in self.channel_tag_score_list]
        feature_word_list = self.get_feature_words()
        self.channel_all_tag_name_list = list(set(tag_name_list + feature_word_list))

    def stat_feature_words(self):
        print('channel id: ', self.channel_id)
        print('channel language: ', self.channel_language)
        self.tag_predictor.stat_feature_words()

    def get_feature_words(self):
        feature_word_list = self.tag_predictor.get_feature_words()
        return feature_word_list

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
                video_tag_score_list = self.channel_video_tag_dict[video_id]
                video_data_dict['predict_tag_list'] = video_tag_score_list
                #video_data_dict['topic_id_list'] = self.tag_predictor.get_topic_id_list()
            video_data_list.append(video_data_dict)
        data['videos'] = video_data_list
        return data
    
    def get_channel_video_tags(self):
        data = {}
        for video_id, video_tag_score_list in self.channel_video_tag_dict.items():
            data[video_id] = video_tag_score_list
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



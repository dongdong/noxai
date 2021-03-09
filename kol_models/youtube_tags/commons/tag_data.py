from enum import IntEnum
from collections import defaultdict

class ItemType(IntEnum):
    CHANNEL = 0
    VIDEO = 1

class TrainTagData(object):
    def __init__(self, item_id, item_type, feature_words):
        self.item_id = item_id
        self.item_type = item_type
        self.feature_words = feature_words
        self.level_1_tag = None
        self.level_2_tag = None

    def get_data(self):
        data = {
            'item_id': self.item_id,
            'item_type': self.item_type,
            'feature_words': self.feature_words,
        }
        if self.level_1_tag:
            data['level_1_tag'] = self.level_1_tag
        if self.level_2_tag:
            data['level_2_tag'] = self.level_2_tag
        return data

    @classmethod
    def From_video_data(cls, video_data, level_1_tag=None, level_2_tag=None):
        tag_data = cls(video_data.video_id, ItemType.VIDEO, 
            video_data.feature_words)
        tag_data.level_1_tag = level_1_tag
        tag_data.level_2_tag = level_2_tag
        return tag_data

    @classmethod
    def From_dict(cls, data_dict):
        tag_data = None
        try:
            item_id = data_dict['item_id']
            item_type = data_dict['item_type']
            feature_words = data_dict['feature_words']
            tag_data = cls(item_id, item_type, feature_words)
            tag_data.level_1_tag = data_dict.get('level_1_tag', None)
            tag_data.level_2_tag = data_dict.get('level_2_tag', None)
        except:
            logging.warn('fail to load train tag data from data dict: %s' % str(data_dict))
        return tag_data    


class PredictTagData(TrainTagData):
    def __init__(self, item_id, item_type, feature_words):
        super().__init__(item_id, item_type, feature_words)
        self.tag_model_level_1_tags = []
        self.tag_model_level_2_tags = []
        self.tag_match_model_tags = []
        self.ytb_topic_tags = []
        self._all_tags = None

    def _get_all_tags(self):
        self._all_tags = []
        tag_score_map = defaultdict(float)
        for tag_name, score in self.tag_model_level_1_tags:
            tag_score_map[tag_name] += score
        for tag_name, score in self.tag_model_level_2_tags:
            tag_score_map[tag_name] += score
        for tag_name, score in self.tag_match_model_tags:
            tag_score_map[tag_name] += score
        for tag_name, score in self.ytb_topic_tags:
            tag_score_map[tag_name] += score
        for tag_name, score in tag_score_map.items():
            if score > 1.0: 
                score = 1.0
            self._all_tags.append((tag_name, score))
        self._all_tags.sort(key=lambda x: x[1], reverse=True)

    @property
    def all_tags(self):
        if self._all_tags is None:
            self._get_all_tags()
        return self._all_tags

    def get_data(self):
        data = super().get_data()
        if self.tag_model_level_1_tags:
            data['tag_model_level_1_tags'] = self.tag_model_level_1_tags
        if self.tag_model_level_2_tags:
            data['tag_model_level_2_tags'] = self.tag_model_level_2_tags
        if self.tag_match_model_tags:
            data['tag_match_model_tags'] = self.tag_match_model_tags
        if self.ytb_topic_tags:
            data['ytb_topic_tags'] = self.ytb_topic_tags
        if self.all_tags:
            data['all_tags'] = self.all_tags
        return data
   
    @classmethod
    def From_channel(cls, channel_id, feature_words):
        tag_data = cls(channel_id, ItemType.CHANNEL, feature_words)
        return tag_data

    @classmethod
    def From_video(cls, video_id, feature_words):
        tag_data = cls(video_id, ItemType.VIDEO, feature_words)
        return tag_data





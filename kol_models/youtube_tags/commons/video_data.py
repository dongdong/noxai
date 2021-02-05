from kol_models.commons.entities import YoutubeVideo
from kol_models.youtube_tags.commons.text_processor import TextParser
from kol_models.youtube_tags.commons.text_processor import TFIDFModel
from kol_models.youtube_tags.commons.text_processor import stop_word_set

VALID_KEYWORDS_MAX_COUNT = 24


class VideoData(YoutubeVideo):
    def __init__(self, video_data_dict):
        super().__init__(video_data_dict)
        if 'feature_words' in video_data_dict:
            self.feature_words = video_data_dict['feature_words']
        else:
            self.feature_words = None
        # too many keywords, may not accurate
        if self.keywords and len(self.keywords) > VALID_KEYWORDS_MAX_COUNT:
            self.keywords = []
    
    @property
    def text(self):
        _text = ''
        if self.title:
            _text += self.title 
        if self.keywords:
            _text += ' ' + ' '.join(self.keywords)
        return _text

    def get_data(self):
        data = super().get_data()
        if self.feature_words:
            data['feature_words'] = self.feature_words
        return data
 

def parse_video_text(video_data, language):
    parser = TextParser(video_data.text, language)
    parser.parse_text()
    feature_words = []
    if parser.token_list:
        feature_words += parser.token_list
    if parser.noun_word_list:
        feature_words += parser.noun_word_list
    if video_data.keywords:
        #feature_words += video_data.keywords
        for keyword in video_data.keywords:
            keyword = keyword.lower().replace('\n', ' ').strip()
            if len(keyword) > 1:
                feature_words.append(keyword)
    feature_words = [word for word in feature_words if word not in stop_word_set]
    video_data.feature_words = feature_words


class VideoTag(object):
    def __init__(self, video_data, level_1_tag=None, level_2_tag=None):
        self.video_data = video_data
        self.level_1_tag = level_1_tag
        self.level_2_tag = level_2_tag



import re

from kol_models.commons.entities import YoutubeVideo
from kol_models.commons.es_utils import get_video_contents
from kol_models.youtube_tags.commons.text_processor import TextParser
from kol_models.youtube_tags.commons.text_processor import TFIDFModel
from kol_models.youtube_tags.commons.text_processor import stop_word_set

VALID_KEYWORDS_MAX_COUNT = 16 #24

def clean_keyword(keyword):
    keyword = keyword.lower() #.replace('\n', ' ').replace('\t', ' ').strip()
    clean_word = re.sub('\W+', ' ', keyword)
    clean_word = re.sub(' +', ' ', clean_word)
    clean_word = clean_word.strip()
    clean_word_no_number = re.sub(r'[0-9 ]', '', clean_word)
    if len(clean_word_no_number) > 1:
        return clean_word
    else:
        return None


class VideoData(YoutubeVideo):
    def __init__(self, video_data_dict):
        super().__init__(video_data_dict)
        if 'feature_words' in video_data_dict:
            self.feature_words = video_data_dict['feature_words']
        else:
            self.feature_words = None
        # too many keywords, may not accurate
        #if self.keywords and len(self.keywords) > VALID_KEYWORDS_MAX_COUNT:
        #    self.keywords = []
        self.keywords = self.keywords[:VALID_KEYWORDS_MAX_COUNT]
        self._clean_keywords = None    

    @property
    def text(self):
        _text = []
        if self.title:
            _text += self.title.split('|')
        #if self.keywords:
        #    _text += self.keywords
        return _text

    def _get_clean_keywords(self):
        clean_keywords = []
        if self.keywords:
            for keyword in self.keywords:
                clean_word = clean_keyword(keyword)
                if clean_word:
                    clean_keywords.append(clean_word)
        return clean_keywords

    @property
    def clean_keywords(self):
        if self._clean_keywords is None:
            self._clean_keywords = self._get_clean_keywords()
        return self._clean_keywords

    def get_data(self):
        data = super().get_data()
        if self.feature_words:
            data['feature_words'] = self.feature_words
        return data

    @staticmethod
    def Load_from_es(video_id):
        video_data = None
        video_contents = get_video_contents(video_id)
        if video_contents:
            video_data = VideoData(video_contents)
        return video_data



def parse_video_text(video_data, language):
    parser = TextParser(video_data.text, language)
    parser.parse_text()
    feature_words = []
    if parser.noun_word_list:
        feature_words += parser.noun_word_list
    feature_words += video_data.clean_keywords
    feature_words = [word for word in feature_words if word not in stop_word_set]
    video_data.feature_words = feature_words


def test_parse_keywords():
    import re
    keyword = '//: 1234 d d:?x(1+2)*3=4我爱=+你！'
    #keyword = '( 1 + 2 ) * 3 = 4'
    #clean_word = re.sub('\W+', ' ', keyword)
    #clean_word = re.sub(' +', ' ', clean_word)
    #clean_word_no_number = re.sub(r'[0-9 ]', '', clean_word)
    print(keyword)
    #print(clean_word)
    #print(clean_word_no_number)
    clean_word = _get_clean_keyword(keyword)
    print(clean_word)


def _test_parse_video_text(video_id_list, language):
    for video_id in video_id_list:
        video_contents = get_video_contents(video_id)
        if video_contents:
            video_data = VideoData(video_contents)
            parse_video_text(video_data, language)
            print(video_data.get_data())


def test():
    #language = 'en'
    language = 'zh'
    #language = 'ja'
    video_id_list = [
        #'8VvnZxyIvG4',
        #'IujuFlOl-SQ',
        #'RQH2Ov0voJE',
        #'QuZlPRrb8FM',
        #'dRNfKWvz7xU',
        'KvUxHZCiJzg',
        'JvRilpE6eps',
        'HYi3roZRN4s',
        'r394BSkvyys',
        '5tQBwbyQ-Fw',
        #'5R1z8_6O3f4',
        #'f-tXq8Us-8E',
        #'GNRe2x9KcT0',
        #'KdR7Pls2o2w',
        #'7ZFxVfqtZms',
        #'41zpY5Eq4HY',
        #'VQPxkviCODw',
    ]
    _test_parse_video_text(video_id_list, language)
            

if __name__ == '__main__':
    test()




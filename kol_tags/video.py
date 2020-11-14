import json
import re
import os
import logging

from text_processor import TFIDFModel, Parser

class VideoData():
    def __init__(self, video_id, title, category_id, keyword_list, description, language): 
        self.video_id = video_id
        self.title = title
        self.category_id = category_id
        self.keyword_list = keyword_list
        self.description = description
        self.language = language
        self.token_list = None
        self.noun_word_list = None
        self.tfidf_word_list = None

    def _get_text(self):
        raw_text = self.title 
        raw_text += '. ' + self.title 
        raw_text += '. ' + self.title 
        raw_text += '. ' + '. '.join(self.keyword_list[:12])
        description_list = self.description.split('\n')
        raw_text += '. ' + ' '.join(description_list[:5])
        return raw_text

    def get_data(self):
        data = {
            'id': self.video_id,
            'title': self.title,
            'category': self.category_id,
            'keywords': self.keyword_list,
            'description': self.description,
            'language': self.language,
        }
        if self.token_list:
            data['tokens'] = self.token_list
        if self.noun_word_list:
            data['noun_words'] = self.noun_word_list
        if self.tfidf_word_list:
            data['tfidf_words'] = self.tfidf_word_list
        return data

    def parse_text(self):
        raw_text = self._get_text()
        self.parser = Parser(raw_text, self.language)
        self.parser.parse_text()
        self.token_list = self.parser.token_list
        self.noun_word_list = self.parser.noun_word_list

    def process_tfidf_words(self, tfidf_model):
        feature_words = self.get_feature_words()
        self.tfidf_word_list = tfidf_model.get_keyword_list(feature_words)

    def _set_tokens(self, token_list):
        self.token_list = token_list

    def _set_noun_words(self, noun_word_list):
        self.noun_word_list = noun_word_list

    def _set_tfidf_words(self, tfidf_words):
        self.tfidf_word_list = tfidf_words

    def get_feature_words(self):
        if self.token_list is None or self.noun_word_list is None:
            self.parse_text()
        return self.token_list + self.noun_word_list

    @staticmethod
    def From_dict(json_obj):
        try:
            video_id = json_obj['id']
            title = json_obj['title']
            category_id = json_obj['category']
            keyword_list = json_obj.get('keywords', [])
            description = json_obj.get('description', '')
            language = json_obj['language']
            video_data = VideoData(video_id, title, category_id, keyword_list, description, language) 
            if 'tokens' in json_obj:
                video_data._set_tokens(json_obj['tokens'])
            if 'noun_words' in json_obj:
                video_data._set_noun_words(json_obj['noun_words'])
            if 'tfidf_words' in json_obj:
                video_data._set_tfidf_words(json_obj['tfidf_words'])
        except:
            logging.warn('Fail to load processed video data from dict, invalid contents %s' % (json_obj))
            video_data = None
        return video_data


class VideoInfo():
    def __init__(self, video_data, level_1_tag=None, level_2_tag=None):
        self.video_data = video_data
        self.level_1_tag = level_1_tag
        self.level_2_tag = level_2_tag

    def get_data(self):
        data = self.video_data.get_data()
        if self.level_1_tag is not None:
            data['level_1'] = self.level_1_tag
        if self.level_2_tag is not None:
            data['level_2'] = self.level_2_tag
        return data
    


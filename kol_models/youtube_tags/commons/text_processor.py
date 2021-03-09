import json
import re
import os
from queue import Queue
import spacy
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
#from konlpy.tag import Kkma
import logging

import kol_models.youtube_tags.commons.path_manager as pm

TFIDF_WORD_COUNT_NO_BELOW = 3

def _load_stop_words():
    stop_words_file_path = pm.get_stop_words_file_path()
    stop_word_set = set()
    with open(stop_words_file_path, 'r') as f:
        for line in f:
            word = line.strip()
            stop_word_set.add(word)
    return stop_word_set

stop_word_set = _load_stop_words()


class TFIDFModel():
    def __init__(self, model_base_dir):
        self.model_base_dir = model_base_dir
        self.dictionary = None
        self.model = None

    def _get_model_path(self):
        return os.path.join(self.model_base_dir, 'model')

    def _get_dictionary_path(self):
        return os.path.join(self.model_base_dir, 'dictionary')

    def train(self, dataset):
        dataset_uniq = [list(set(doc)) for doc in dataset]
        self.dictionary = Dictionary(dataset_uniq)
        self.dictionary.filter_extremes(no_below=TFIDF_WORD_COUNT_NO_BELOW)
        corpus = [self.dictionary.doc2bow(doc) for doc in dataset]
        self.model = TfidfModel(corpus)
   
    def get_vector(self, doc):
        bow = self.dictionary.doc2bow(doc)
        vec = self.model[bow]
        return vec

    def get_dictionary_size(self):
        if self.dictionary:
            return len(self.dictionary)
        else:
            return 0

    def get_keyword_list(self, doc, max_len=16):
        keyword_list = []
        tfidf_vec = self.get_vector(doc)
        tfidf_vec_sorted = sorted(tfidf_vec, key=lambda v: v[1], reverse=True)        
        for (pos, score) in tfidf_vec_sorted[:max_len]:
            keyword_list.append((self.dictionary.get(pos), score))
        return keyword_list

    def save_model(self):
        succ = False
        if self.model != None and self.dictionary != None:
            dictionary_path = self._get_dictionary_path()
            self.dictionary.save_as_text(dictionary_path)
            model_path = self._get_model_path()
            self.model.save(model_path)
            logging.info("save model, path:" + model_path)
            succ = True
        else:
            logging.warn("save model error: no model to save!")
        return succ

    def load_model(self):
        succ = False
        model_path = self._get_model_path()
        dictionary_path = self._get_dictionary_path()
        if os.path.exists(model_path) and os.path.exists(dictionary_path):
            self.dictionary = Dictionary.load_from_text(dictionary_path)
            self.model = TfidfModel.load(model_path)
            logging.info("TFIDFModel load model, path:" + model_path)
            succ = True
        else:
            logging.warn("TFIDFModel load model error: no model to load!")
        return succ       
    

class ParserEn():
    nlp = None
    
    def _load_nlp(self):
        if ParserEN.nlp is None:
            ParserEN.nlp = spacy.load("en_core_web_sm")

    def __init__(self, raw_text_list):
        self.raw_text_list = raw_text_list
        self.noun_word_list = []
        self._load_nlp()

    def _clean_text(self, text):
        #print(text)
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
        text = re.sub(r'^[0-9a-zA-Z_]{0,19}@[0-9a-zA-Z]{1,13}\.[com,cn,net]{1,3}$', ' ', text)
        text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip() 
        text = text.lower()
        text = re.sub('  +', ', ', text)
        text_list = text.split(', ')
        #print(text)
        #print(text_list)
        return text_list

    def _get_doc(self, clean_text):
        return ParserEn.nlp(clean_text)

    def _get_noun_chunks(self, doc):
        noun_chunk_list = [chunk.text for chunk in doc.noun_chunks] 
        #print(noun_chunk_list)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_word_num = 6
        words = [word for word in words 
                if len(word) > min_length 
                and len(word.split(' ')) <= max_word_num]
        return words

    def _get_noun_words(self, doc):
        noun_chunk_list = self._get_noun_chunks(doc)
        noun_words = self._noun_words_filter(noun_chunk_list)
        return noun_words
        
    def parse_text(self):
        for raw_text in self.raw_text_list:
            clean_text_list = self._clean_text(raw_text)
            for clean_text in clean_text_list:
                doc = self._get_doc(clean_text)
                self.noun_word_list += self._get_noun_words(doc)


class ParserZh(ParserEn):

    def _load_nlp(self):
        if ParserEN.nlp is None:
            #ParserEN.nlp = spacy.load("en_core_web_sm")
            ParserZh.nlp = spacy.load("zh_core_web_sm")

    def _clean_text(self, text):
        text = re.sub(u'(?:[^\u4e00-\u9fa5a-zA-Z0-9])', u' ', text.lower()).strip()
        #text = re.sub(u'(?:[^\u4e00-\u9fa5])', u' ', text).strip()
        #text = re.sub(' +', ' ', text)    
        #print(text)
        #text = re.sub('  +', ', ', text)
        #text_list = text.split(', ')
        text_list = [text]
        return text_list

    def _get_doc(self, clean_text):
        return ParserZh.nlp(clean_text)

    def _get_noun_chunks(self, doc):
        noun_chunk_list = []
        noun_tags = set(['NN', 'NR'])
        for token in doc:
            if token.tag_ in noun_tags and len(token.text) > 1:
                noun_chunk_list.append(token.text)
        #print(noun_chunk_list)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_length = 8
        words = [word for word in words 
                    if len(word) >= min_length 
                    and len(word) <= max_length]
        return words


class ParserJa(ParserEn):
    def _clean_text(self, text):
        text = re.sub('\W+', ' ', text.lower())
        text = re.sub(' +', ' ', text).strip()
        #print(text)
        text_list = [text]
        #print(text_list)
        return text_list

    def _get_doc(self, clean_text):
        return clean_text        

    def _get_noun_chunks(self, doc):
        noun_chunk_list = doc.split(' ')
        #print(noun_chunk_list)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_length = 12
        words = [word for word in words 
                    if len(word) >= min_length 
                    and len(word) <= max_length]
        return words


class ParserKo(ParserJa):
    def _clean_text(self, text):
        #print(text)
        #text = re.sub('\W+', ' ', text.lower())
        text = re.sub(u"(?:[^\uac00-\ud7ffa-z0-9])", u' ', text.lower()).strip()
        text = re.sub('  +', ', ', text)
        #print(text)
        text_list = text.split(', ')
        #print(text_list)
        return text_list

    def _get_doc(self, clean_text):
        return clean_text        

    def _get_noun_chunks(self, doc):
        noun_chunk_list = [doc]
        #print(noun_chunk_list)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_word_num = 6
        words = [word for word in words 
                if len(word) > min_length 
                and len(word.split(' ')) <= max_word_num]
        return words


class ParserDefault(ParserEn):
    def _clean_text(self, text):
        text_list = [text]
        return text_list

    def _get_doc(self, clean_text):
        return clean_text        

    def _get_noun_chunks(self, doc):
        noun_chunk_list = []
        return noun_chunk_list

    def _noun_words_filter(self, words):
        return words

   
class TextParser():
    def __init__(self, raw_text, language):
        #self.language = language
        self._init_parser_list(raw_text, language)
        self.noun_word_list = []
    def _init_parser_list(self, raw_text, language):
        self.parser_list = []
        if language is None:
            self.parser_list.append(ParserDefault(raw_text))
        elif language == 'en':
            self.parser_list.append(ParserEn(raw_text))
        elif language == 'zh' or language == 'zh-Hant' or language == 'zh-Hans':
            self.parser_list.append(ParserEn(raw_text))
            self.parser_list.append(ParserZh(raw_text))
        elif language == 'ko':
            self.parser_list.append(ParserEn(raw_text))
            self.parser_list.append(ParserKo(raw_text))
        elif language == 'ja':
            self.parser_list.append(ParserEn(raw_text))
            self.parser_list.append(ParserJa(raw_text))
        else:
            self.parser_list.append(ParserDefault(raw_text))
    def parse_text(self):
        for parser in self.parser_list:
            parser.parse_text()
            self.noun_word_list += parser.noun_word_list


def test_parser():
    raw_text = [
        'Serial Season 3 Episode 04: A Bird in Jail Is Worth Two on the Street',
        'The Generation Why #311 - Who Kil.l.ed  Jerry Tobias',
        'Bollywood bikini',
        'Indian actress bikini',
        'Savdhaan India 5th February 2019 full episode',
        'INTERNET GRATIS MOVISAR,TUENTI Y BITEL PARA JUEGOS ONLINE',
    ]
    parser = ParserEn(raw_text)
    parser.parse_text()
    print(parser.noun_word_list)

if __name__ == '__main__':
    test_parser()




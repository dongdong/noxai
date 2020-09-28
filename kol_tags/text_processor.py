import json
import re
import os
from queue import Queue
import spacy
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
import logging

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
        self.dictionary.filter_extremes(no_below=10)
        corpus = [self.dictionary.doc2bow(doc) for doc in dataset]
        self.model = TfidfModel(corpus)
   
    def get_vector(self, doc):
        bow = self.dictionary.doc2bow(doc)
        vec = self.model[bow]
        return vec

    def get_dictionary_size(self):
        return len(self.dictionary)
 
    def get_keyword_list(self, doc, max_len=24):
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
            logging.info("load model, path:" + model_path)
            succ = True
        else:
            logging.warn("load model error: no model to load!")
        return succ       


class ParserEn():
    nlp = spacy.load("en_core_web_sm")

    def __init__(self, raw_text):
        self.raw_text = raw_text
        self.token_list = []
        self.noun_word_list = []
        self.entity_list = []

    def _clean_text(self, text):
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
        text = re.sub(r'^[0-9a-zA-Z_]{0,19}@[0-9a-zA-Z]{1,13}\.[com,cn,net]{1,3}$', ' ', text)
        text = re.sub(r'[^a-zA-Z]', ' ', text).strip() 
        text = re.sub(' {2,}', ', ', text)
        text = text.lower()
        return text

    def _get_doc(self, clean_text):
        return ParserEn.nlp(clean_text)

    def _get_tokens(self, doc):
        pos_filter_set = ['PUNCT']
        return [token.text for token in doc 
                if token.pos_ not in pos_filter_set 
                    and not token.is_stop
                    and len(token.text) > 2]

    def _get_noun_chunks(self, doc):
        noun_chunk_list = [chunk.text for chunk in doc.noun_chunks] 
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
        
    def _get_entities(self, doc):
        return []

    def parse_text(self):
        clean_text = self._clean_text(self.raw_text)
        doc = self._get_doc(clean_text)
        self.token_list = self._get_tokens(doc)
        self.noun_word_list = self._get_noun_words(doc)
        self.entity_list = self._get_entities(doc)


class ParserZh(ParserEn):
    nlp = spacy.load("zh_core_web_sm")

    def _clean_text(self, text):
        text = re.sub(u'(?:[^\u4e00-\u9fa5])', u' ', text).strip()
        text = re.sub(' +', ', ', text)    
        return text

    def _get_doc(self, clean_text):
        return ParserZh.nlp(clean_text)

    def _get_tokens(self, doc):
        pos_filter_set = ['PUNCT']
        return [token.text for token in doc 
                if token.pos_ not in pos_filter_set 
                    and not token.is_stop
                    and len(token.text) > 1]

    def _get_noun_chunks(self, doc):
        noun_chunk_list = []
        q = Queue(maxsize=20)
        #print([(token.text, token.pos_, token.tag_) for token in doc])
        noun_tags = set(['NN', 'NR'])
        for token in doc:
            if token.tag_ in noun_tags and not q.full():
                q.put(token.text)
                noun_chunk_list.append(token.text)
            else:
                if not q.empty():
                    q_size = 1
                    noun_word = q.get()
                    noun_chunk = noun_word
                    last_word = noun_word
                    while not q.empty():
                        #noun_chunk += q.get() 
                        q_size += 1
                        noun_word = q.get()
                        noun_pair = last_word + noun_word
                        noun_chunk_list.append(noun_pair)
                        last_word = noun_word
                        noun_chunk += noun_word
                    if q_size > 2: # single and noun_pair already added
                        noun_chunk_list.append(noun_chunk)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_length = 6
        words = [word for word in words 
                    if len(word) >= min_length 
                    and len(word) <= max_length]
        return words


class Parser():
    def __init__(self, raw_text, language):
        #self.language = language
        self._init_parser_list(raw_text, language)
        self.token_list = []
        self.noun_word_list = []
        self.entity_list = []
    def _init_parser_list(self, raw_text, language):
        self.parser_list = [ParserEn(raw_text)]
        if language == 'zh' or language == 'zh-Hans' or language == 'zh-Hant':
            self.parser_list.append(ParserZh(raw_text))
    def parse_text(self):
        for parser in self.parser_list:
            parser.parse_text()
            self.token_list += parser.token_list
            self.noun_word_list += parser.noun_word_list
            self.entity_list += parser.entity_list


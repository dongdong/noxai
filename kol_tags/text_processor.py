import json
import re
import os
from queue import Queue
import spacy
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from konlpy.tag import Kkma

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
        if self.dictionary:
            return len(self.dictionary)
        else:
            return 0

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
            logging.info("TFIDFModel load model, path:" + model_path)
            succ = True
        else:
            logging.warn("TFIDFModel load model error: no model to load!")
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
        pos_filter_set = set(['PUNCT'])
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


class ParserKo(ParserEn):
    nlp = Kkma()

    def _clean_text(self, text):
        text = re.sub(u"(?:[^\uac00-\ud7ff])", u' ', text).strip()
        #text = re.sub(' +', ' ', text)    
        return text

    def _get_doc(self, clean_text):
        #pos_tag = ParserKo.nlp.pos(clean_text)
        #print(pos_tag)
        #nouns_list = ParserKo.nlp.nouns(clean_text) 
        #print(nouns_list)   
        #print(clean_text)
        return clean_text    

    def _get_tokens(self, doc):
        pos_filter_set = set(['SP'])
        token_list = [word for (word, tag) in ParserKo.nlp.pos(doc) 
            if tag not in pos_filter_set and len(word) > 1]
        return token_list

    def _get_noun_chunks(self, doc):
        noun_chunk_list = ParserKo.nlp.nouns(doc)
        return noun_chunk_list

    def _noun_words_filter(self, words):
        min_length = 2
        max_length = 10
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
        elif language == 'ko':
            self.parser_list.append(ParserKo(raw_text))
        else:
            pass
    def parse_text(self):
        for parser in self.parser_list:
            parser.parse_text()
            self.token_list += parser.token_list
            self.noun_word_list += parser.noun_word_list
            self.entity_list += parser.entity_list


def test_parser_ko():
    text = '''
지금까지 방송에서 보여드리지 못했던 모습, 나누지 못했던 이야기들 하나씩 풀어가려고 합니다~
자주만나요!^^

촬영 죠슈아신스튜디오
편집 임혜민
음악 차정민
제작 TKC

안녕하세요 여러분~~! 
함연지의 과거 여행 제 2탄, 유년시절 썰을 가지고 왔어요!
오늘 저의 너무너무 소중한 가족 앨범의 사진들을 보여 드리면서 추억을 하나하나 되짚어 보았는데요.ð
부모님과의 인터뷰를 통해서 어린 시절 저의 성격, 태몽, 훈육 방법, 제 이름이 왜 연지인지 등등!!! 
여러분들이 남겨주셨던 질문에 대한 Q&A 시간도 가져보았습니다~!

오늘 방송된 MBC '전지적 참견 시점'에도 이 영상을 촬영하는 장면이 나왔어요 헤헤!
'전지적 참견 시점'도 많은 시청 바랍니당~! 감사합니다!!

시청해 주셔서 감사합니다!
맛있는 하루 보내세요! 

햄연지의 더 많은 일상이 궁금하시다면?
ðhttps://www.instagram.com/yonjiham/

[햄연지(함연지) 공식 유튜브 채널]
제작
ð(주)샌드박스네트워크
비즈니스 이메일
ðyonjiham@sandbox.co.kr

Music provided by Bgm President
Track : Playful Kitten - https://youtu.be/1GXe7LPxN_k
Track : Everyday Picnic  - https://youtu.be/h0HtDfVzIys
Track : a Little Clown at Champs-Elysees  - https://youtu.be/zw23_hWjmuY
    '''

    language = 'ko'
    parser = Parser(text, language)
    words = parser.parse_text()
    #print(parser.token_list)
    #print(parser.noun_word_list)



if __name__ == '__main__':
    test_parser_ko()




import os
import json
import time
import logging
from collections import defaultdict, Counter
import traceback

from tag import TagIndex, tag_name_config_list
import path_manager as pm

from gensim.models import Word2Vec, KeyedVectors

import numpy as np
from scipy.sparse import coo_matrix

from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB, ComplementNB
import pickle


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')


class FeatureData():
    def __init__(self, data_X, rows_X, cols_X, num_rows_X, num_cols_X, data_y):
        self.data_X = data_X
        self.rows_X = rows_X
        self.cols_X = cols_X
        self.num_rows_X = num_rows_X
        self.num_cols_X = num_cols_X
        self.data_y = data_y
        self.X = coo_matrix((data_X, (rows_X, cols_X)), shape=(num_rows_X, num_cols_X))
        self.y = np.array(data_y)

    def save(self, path):
        data = {
            'data_X': self.data_X,
            'rows_X': self.rows_X,
            'cols_X': self.cols_X,
            'num_rows_X': self.num_rows_X,
            'num_cols_X': self.num_cols_X,
            'data_y': self.data_y,
        }
        with open(path, 'w') as f:
            json.dump(data, f)

    @staticmethod
    def Load(path):
        with open(path, 'r') as f:
            try:
                data = json.load(f)
            except:
                logging.error('load train data error! return. data path: ' % path)
                return None

        data_X = data['data_X']
        rows_X = data['rows_X']
        cols_X = data['cols_X']
        num_rows_X = data['num_rows_X']
        num_cols_X = data['num_cols_X']
        data_y = data['data_y']
        feature_data = FeatureData(data_X, rows_X, cols_X, num_rows_X, num_cols_X, data_y)
        return feature_data


class TrainData():
    def __init__(self, X, y):
        self.X = X
        self.y = y
        self.train_X, self.test_X, self.train_y, self.test_y = train_test_split(X, y, 
                test_size=0.25, random_state=666)

    def get_train_data(self):
        return self.train_X, self.train_y

    def get_test_data(self):
        return self.test_X, self.test_y
    

class TagMatchModel():
    def __init__(self, language):
        self.match_words_config = {
            'Unboxing': ['unboxing', '开箱', '開箱', '언박싱'], 
            'VLOG': ['vlog', 'vloger', 'vlogging', 'vlog', 'vlogs', '브이로그'], 
            'ASMR': ['asmr'],
            'MUKBANG': ['mukbang', '먹방'],
        }
        self.match_word_tag_map = {}
        for item in tag_name_config_list:
            en_name = item['en']
            if en_name in self.match_words_config:
                if language not in item:
                    continue
                tag_name = item[language]
                match_word_list = self.match_words_config[en_name]
                for match_word in match_word_list:
                    self.match_word_tag_map[match_word] = tag_name    

    def get_tag_class_list(self, keyword_list):
        tag_set = set()
        for word in keyword_list:
            word = word.lower()
            if word in self.match_word_tag_map:
                tag_name = self.match_word_tag_map[word]
                tag_set.add(tag_name)
        return list(tag_set)


class TagModel():
    def __init__(self):
        self.tag_1_model = None
        self.tag_2_model = None
        self.tag_1_default_prob_map = None
        self.tag_2_default_prob_map = None

    def train(self, X, y):
        tag_1_y = y[:, 0]
        self.tag_1_model = ComplementNB()
        self.tag_1_model.fit(X, tag_1_y)
        tag_1_score = self.tag_1_model.score(X, tag_1_y) 
        tag_2_y = y[:, 1]
        self.tag_2_model = ComplementNB()
        self.tag_2_model.fit(X, tag_2_y)
        tag_2_score = self.tag_2_model.score(X, tag_2_y) 
        logging.info('train tag model. tag_1 model score: %f, tag_2 model score: %f' % 
               (tag_1_score, tag_2_score))
    
        tag_1_y_counter = Counter(tag_1_y)        
        tag_2_y_counter = Counter(tag_2_y)
        total = len(y)
        self.tag_1_default_prob_map = {str(tag):count/total for tag, count in tag_1_y_counter.items()}
        self.tag_2_default_prob_map = {str(tag):count/total for tag, count in tag_2_y_counter.items()}

    def evaluate(self, X, y):
        tag_1_y = y[:, 0]
        tag_1_score = self.tag_1_model.score(X, tag_1_y) 
        tag_2_y = y[:, 1]
        tag_2_score = self.tag_2_model.score(X, tag_2_y) 
        logging.info('evaluate tag model. tag_1 model score: %f, tag_2 model score: %f' % 
                (tag_1_score, tag_2_score))
    
    def predict_batch(self, X):
        tag_1_y_prob_arr = self.tag_1_model.predict_proba(X)
        tag_2_y_prob_arr = self.tag_2_model.predict_proba(X)
        return tag_1_y_prob_arr, tag_2_y_prob_arr

    def get_top_prob_class_list_batch(self, X, tag_1_size=3, tag_2_size=3):
        top_list = []
        tag_1_y_prob_arr, tag_2_y_prob_arr = self.predict_batch(X)
        tag_1_end_pos = -(tag_1_size + 1)
        tag_2_end_pos = -(tag_2_size + 1)
        for tag_1_y_prob, tag_2_y_prob in zip(tag_1_y_prob_arr, tag_2_y_prob_arr):
            tag_1_top_index_list = tag_1_y_prob.argsort()[:tag_1_end_pos:-1]
            tag_1_top_class_list = [self.tag_1_model.classes_[tag_1_top_index] for tag_1_top_index in tag_1_top_index_list]
            tag_1_top_prob_list = [tag_1_y_prob[tag_1_top_index] for tag_1_top_index in tag_1_top_index_list]
            tag_2_top_index_list = tag_2_y_prob.argsort()[:tag_2_end_pos:-1]
            tag_2_top_class_list = [self.tag_2_model.classes_[tag_2_top_index] for tag_2_top_index in tag_2_top_index_list]
            tag_2_top_prob_list = [tag_2_y_prob[tag_2_top_index] for tag_2_top_index in tag_2_top_index_list]
            top_list.append((tag_1_top_class_list, tag_1_top_prob_list, tag_2_top_class_list, tag_2_top_prob_list))
        return top_list


    def get_valid_tag_list(self, pred_result):
        if not pred_result:
            return [], []

        tag_1_class_score_list, tag_2_class_score_list = [], []
        tag_1_pred_class_list, tag_1_pred_prob_list, tag_2_pred_class_list, tag_2_pred_prob_list = pred_result
        
        for tag_1_pred_class, tag_1_pred_prob in zip(tag_1_pred_class_list, tag_1_pred_prob_list):
            tag_1_default_prob = self.get_tag_1_default_prob(tag_1_pred_class)
            tag_1_prob_threshold = tag_1_default_prob * 2
            if tag_1_pred_prob > tag_1_prob_threshold:
                tag_1_class_score = (tag_1_pred_prob - tag_1_prob_threshold) / tag_1_default_prob
                tag_1_class_score_list.append((tag_1_pred_class, tag_1_class_score))

        for tag_2_pred_class, tag_2_pred_prob in zip(tag_2_pred_class_list, tag_2_pred_prob_list):
            tag_2_default_prob = self.get_tag_2_default_prob(tag_2_pred_class)
            tag_2_prob_threshold = tag_2_default_prob * 2
            if tag_2_pred_prob > tag_2_prob_threshold:
                tag_2_class_score = (tag_2_pred_prob - tag_2_prob_threshold) / tag_2_default_prob
                tag_2_class_score_list.append((tag_2_pred_class, tag_2_class_score))
        
        return tag_1_class_score_list, tag_2_class_score_list

    def get_tag_1_default_prob(self, tag_1_class):
        return self.tag_1_default_prob_map[str(tag_1_class)]

    def get_tag_2_default_prob(self, tag_2_class):
        return self.tag_2_default_prob_map[str(tag_2_class)]
    
    def _get_tag_1_model_file_path(self, base_dir):
        return os.path.join(base_dir, 'tag_1.pickle')

    def _get_tag_2_model_file_path(self, base_dir):
        return os.path.join(base_dir, 'tag_2.pickle')

    def _get_meta_data_file_path(self, base_dir):
        return os.path.join(base_dir, 'meta.json')

    def save_model(self, model_dir):
        tag_1_model_path = self._get_tag_1_model_file_path(model_dir)
        tag_2_model_path = self._get_tag_2_model_file_path(model_dir)
        logging.info('save tag_1 model, path: %s' % (tag_1_model_path))
        with open(tag_1_model_path, 'wb') as f_1:
            pickle.dump(self.tag_1_model, f_1)
        logging.info('save tag_2 model, path: %s' % (tag_2_model_path))
        with open(tag_2_model_path, 'wb') as f_2:
            pickle.dump(self.tag_2_model, f_2)
        
        meta_data_path = self._get_meta_data_file_path(model_dir)
        meta_data = {
            'tag_1_default_prob_map': self.tag_1_default_prob_map,
            'tag_2_default_prob_map': self.tag_2_default_prob_map,
        }
        with open(meta_data_path, 'w') as f:
            json.dump(meta_data, f)

    def load_model(self, model_dir):
        tag_1_model_path = self._get_tag_1_model_file_path(model_dir)
        tag_2_model_path = self._get_tag_2_model_file_path(model_dir)
        try:
            logging.info('load tag_1 model from path: %s' % (tag_1_model_path))
            with open(tag_1_model_path, 'rb') as f_1:
                self.tag_1_model = pickle.load(f_1)
            logging.info('load tag_2 model from path: %s' % (tag_2_model_path))
            with open(tag_2_model_path, 'rb') as f_2:
                self.tag_2_model = pickle.load(f_2)
        
            meta_data_path = self._get_meta_data_file_path(model_dir)
            with open(meta_data_path, 'r') as f:
                meta_data = json.load(f)
                self.tag_1_default_prob_map = meta_data['tag_1_default_prob_map']
                self.tag_2_default_prob_map = meta_data['tag_2_default_prob_map']
        except:
            logging.warn('TagModel failed to load model. Exception:' + traceback.format_exc())


def load_train_data(feature_data_path):
    feature_data = FeatureData.Load(feature_data_path)
    train_data = TrainData(feature_data.X, feature_data.y)
    return train_data


def train_model(language, train_data, model_dir=None): 
    train_X, train_y = train_data.get_train_data()
    test_X, test_y = train_data.get_test_data()
    
    tag_model = TagModel()
    tag_model.train(train_X, train_y)
    tag_model.evaluate(test_X, test_y)

    if model_dir is not None:
        tag_model.save_model(model_dir)
    
    return tag_model


def load_model(model_dir):
    tag_model = TagModel()
    tag_model.load_model(model_dir)
    return tag_model


def check_result_multi_tags(train_data, tag_model, test_size=1000):
    true_1_count = 0
    true_2_count = 0
    unknown_1_count = 0
    unknown_2_count = 0

    test_X, test_y = train_data.get_test_data()
    X = test_X[:test_size]
    y_true_arr = test_y[:test_size]
    result_list = tag_model.get_top_prob_class_list_batch(X)
    for i in range(test_size):
        y_true = y_true_arr[i]
        y_1_true_class = y_true[0]
        y_1_true_tag = y_1_true_class
        y_2_true_class = y_true[1]
        y_2_true_tag = y_2_true_class

        y_1_pred_class_prob_list, y_2_pred_class_prob_list = tag_model.get_valid_tag_list(result_list[i])
        y_1_pred_name_list = y_1_pred_class_prob_list
        y_2_pred_name_list = y_2_pred_class_prob_list
        y_1_pred_class_list = [pred_class for pred_class, prob in y_1_pred_class_prob_list]
        y_2_pred_class_list = [pred_class for pred_class, prob in y_2_pred_class_prob_list]

        if len(y_1_pred_class_list) > 0:
            predict_right = False
            if y_1_true_class in y_1_pred_class_list:
                predict_right = True
                true_1_count +=  1
            print('level_1 tag: ', y_1_true_tag, ' predcit tags: ', y_1_pred_name_list, predict_right)
        else:
            unknown_1_count += 1
            print('level_1 tag: ', y_1_true_tag, ' predcit tags: ', y_1_pred_name_list)

        if len(y_2_pred_class_list) > 0:
            predict_right = False
            if y_2_true_class in y_2_pred_class_list:
                predict_right = True
                true_2_count +=  1
            print('level_2 tag: ', y_2_true_tag, ' predcit tags: ', y_2_pred_name_list, predict_right)
        else:
            unknown_2_count += 1
            print('level_2_tag: ', y_2_true_tag, ' predict tags: ', y_2_pred_name_list)
        
    print('level 1 true:', true_1_count / test_size)
    print('level 2 true:', true_2_count/ test_size)
    print('level 1 unknown:', unknown_1_count / test_size)
    print('level 2 unknown:', unknown_2_count / test_size)
    print('predict level 1 precision:', true_1_count / (test_size - unknown_1_count))
    print('predict level 2 precision:', true_2_count/ (test_size - unknown_2_count))


def train_tag_model(language):
    feature_data_path = pm.get_tag_video_feature_data_path(language)
    model_dir = pm.get_tag_train_model_dir(language)
    train_data = load_train_data(feature_data_path)
    tag_model = train_model(language, train_data, model_dir)


def load_and_evaluate_model(language):
    model_dir = pm.get_tag_train_model_dir(language)
    feature_data_path = pm.get_tag_video_feature_data_path(language)
    train_data = load_train_data(feature_data_path)
    tag_model = load_model(model_dir)
    check_result_multi_tags(train_data, tag_model)


def test_tag_model():
    language = 'en'
    #language = 'zh-Hans'
    #language = 'zh-Hant'
    #language = 'ko'
    train_tag_model(language)
    load_and_evaluate_model(language)


def test_tag_match_model():
    language = 'en'
    #language = 'zh-Hans'
    #language = 'zh-Hant'
    tag_match_model = TagMatchModel(language)
    print(tag_match_model.match_word_tag_map)
        

def get_tag_model_features(model, dictionary, max_len):
    tag_class_range, feature_word_range = model.feature_log_prob_.shape
    #print(tag_class_range, feature_word_range)

    tag_class_feature_words_map = {}
    for tag_class_index in range(tag_class_range):
        tag_class = model.classes_[tag_class_index]
        feature_value = model.feature_log_prob_[tag_class_index]
        top_feature_vocab_index_list = feature_value.argsort()[:-(max_len+1):-1]
        top_feature_word_list = [dictionary[i] for i in top_feature_vocab_index_list] 
        #print(tag_class, top_feature_word_list)
        tag_class_feature_words_map[tag_class] = top_feature_word_list

    return tag_class_feature_words_map


def test_tag_model_features():
    from text_processor import TFIDFModel
    #language = 'zh-Hant'
    language = 'en'
    tfidf_model_path = pm.get_tfidf_inference_model_dir(language)
    tag_model_path = pm.get_tag_inference_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_path)
    tfidf_model.load_model()
    tag_model = TagModel()
    tag_model.load_model(tag_model_path)

    #tag_class_feature_words_map_1 = get_tag_model_features(tag_model.tag_1_model, tfidf_model.dictionary, 200)
    tag_class_feature_words_map_2 = get_tag_model_features(tag_model.tag_2_model, tfidf_model.dictionary, 64)

    word_tag_counter = defaultdict(int)    
    #for k, v_list in tag_class_feature_words_map_1.items():
    #    for v in v_list:
    #        word_tag_counter[v] += 1
    for k, v_list in tag_class_feature_words_map_2.items():
        for v in v_list:
            word_tag_counter[v] += 1

    black_list = set([k for k, v in word_tag_counter.items() if v > 3])
    print(black_list)

    #for k, v_list in tag_class_feature_words_map_1.items():
    #    #print(k)
    #    #print(v)
    #    if k == '': 
    #        continue
    #    for v in v_list:
    #        if v not in black_list:
    #            print("%s##%s" % (k, v))
    for k, v_list in tag_class_feature_words_map_2.items():
        #print(k)
        #print(v)
        for v in v_list:
            if v not in black_list:
                print("%s##%s" % (k, v))

    '''
    all_feature_words = set()
    for feature_word_list in tag_class_feature_words_map_1.values():
        all_feature_words.update(feature_word_list)
    for feature_word_list in tag_class_feature_words_map_2.values():
        all_feature_words.update(feature_word_list)
    
    for word in all_feature_words:
        print(word)
    '''

if __name__ == '__main__':
    test_tag_model()
    #test_tag_model_features()





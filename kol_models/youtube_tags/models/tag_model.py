import os
import pickle
import traceback
import logging
from enum import IntEnum

from collections import defaultdict, Counter
from sklearn.model_selection import train_test_split

import kol_models.youtube_tags.commons.path_manager as pm
from kol_models.youtube_tags.train.feature_data_process import FeatureData 
from kol_models.youtube_tags.train.feature_data_process import get_tfidf_feature_data_from_tag_data_iter
from kol_models.youtube_tags.commons.video_data import VideoData
from kol_models.youtube_tags.commons.text_processor import TFIDFModel


class TrainData():
    def __init__(self, X, y, test_size=0.25):
        self.X = X
        self.y = y
        self.train_X, self.test_X, self.train_y, self.test_y = train_test_split(X, y, 
                test_size=test_size, random_state=666)
    def get_train_data(self):
        return self.train_X, self.train_y
    def get_test_data(self):
        return self.test_X, self.test_y
 

class TagLevel(IntEnum):
    LEVEL_1_TAG = 0
    LEVEL_2_TAG = 1


class TagModel(object):

    model_file_name_config_list = ['level_1_tag.pickle', 'level_2_tag.pickle']

    def __init__(self):
        self.model = None
        self.model_path = None
        self._min_prob_threshold = None

    def _new_model(self):
        pass

    def train(self, X, y):
        logging.info('train tag model. X shape: %s, y shape: %s'
                % (str(X.shape), str(y.shape)))
        self.model = self._new_model()
        self.model.fit(X, y)
        score = self.model.score(X, y)
        logging.info('train tag model. score: %f' % (score))

    def evaluate(self, X, y):
        logging.info('evaluate tag model. X shape: %s, y shape: %s'
                % (str(X.shape), str(y.shape)))
        assert self.model is not None
        score = self.model.score(X, y)
        logging.info('evaluate tag model. score: %f' % (score))

    def predict_batch(self, X):
        assert self.model is not None
        y_prob_arr = self.model.predict_proba(X)        
        return y_prob_arr
    
    def _get_min_prob_threshold(self):
        if not self._min_prob_threshold:
            tag_size = len(self.model.classes_)
            avg_prob = 1.0 / tag_size
            self._min_prob_threshold =  avg_prob * 1.5 #1.2
        return self._min_prob_threshold

    def _get_top_tag_list(self, y_prob, size):
        top_index_list = y_prob.argsort()[:-(size+1):-1]
        top_tag_list = [(self.model.classes_[tag_index], y_prob[tag_index])
                for tag_index in top_index_list]
        # filter invalid or low prob tags
        highest_prob_tag = top_tag_list[0][0]
        if highest_prob_tag == '':
            top_tag_list = []
        else:
            min_prob = self._get_min_prob_threshold()
            highest_prob = top_tag_list[0][1]
            top_tag_list = [(tag, prob) for tag, prob in top_tag_list 
                    if tag != ''
                        and prob > min_prob 
                        and prob * 2 > highest_prob]
        return top_tag_list

    def get_top_prob_tags_batch(self, X, size=3):
        assert self.model is not None
        #logging.info('get top prob tags. data shape: %s' % (str(X.size)))
        y_prob_arr = self.predict_batch(X)
        top_list = [self._get_top_tag_list(y_prob, size)
                for y_prob in y_prob_arr]
        return top_list

    def load(self):
        assert self.model_path is not None
        logging.info('load tag model from path: %s' % self.model_path)
        succ = True
        try:
            with open(self.model_path, 'rb') as f:
                self.model = pickle.load(f)
        except:
            logging.error('TagModel_NB failed to load model. Exception: ' 
                    + traceback.format_exc()) 
            succ = False
        return succ

    def save(self):
        assert self.model_path is not None
        logging.info('save tag model to path: %s' % self.model_path)
        if self.model is not None:
            with open(self.model_path, 'wb') as f:
                pickle.dump(self.model, f)
        else:
            logging.warn('TagModel_NB No trained model to save!')

    def get_tag_feature_weights_dict(self, size=64):
        assert self.model is not None
        tag_size, feature_size = self.model.feature_log_prob_.shape
        tag_feature_dict = {}
        for tag_index in range(tag_size):
            tag_name = self.model.classes_[tag_index]
            feature_value = self.model.feature_log_prob_[tag_index]
            top_feature_index = feature_value.argsort()[:-(size+1):-1]
            feature_weights = [(i, feature_value[i]) for i in top_feature_index] 
            tag_feature_dict[tag_name] = feature_weights
        return tag_feature_dict

    @classmethod
    def Get_model(cls, language, tag_level, is_train):
        model_file_name = cls.model_file_name_config_list[tag_level]
        model = cls()
        model.model_path = pm.get_tag_model_path(language, is_train, model_file_name)
        return model


def get_models(language, is_train):
    #from kol_models.youtube_tags.models.tag_model_nb import TagModel_NB as TagModelTest
    #from kol_models.youtube_tags.models.tag_model_lr import TagModel_LR as TagModelTest
    from kol_models.youtube_tags.models.tag_model_ensemble import TagModel_Ensemble as TagModelTest
    #from kol_models.youtube_tags.models.tag_model_nb_ensemble import TagModel_NB_Ensemble as TagModelTest
    level_1_tag_model = TagModelTest.Get_model(language, TagLevel.LEVEL_1_TAG, is_train)
    level_2_tag_model = TagModelTest.Get_model(language, TagLevel.LEVEL_2_TAG, is_train)
    if is_train:
        tfidf_model_dir = pm.get_train_tfidf_model_dir(language)
    else:
        tfidf_model_dir = pm.get_inference_tfidf_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    return level_1_tag_model, level_2_tag_model, tfidf_model


def load_models(language, is_train):
    level_1_tag_model, level_2_tag_model, tfidf_model = get_models(language, is_train)
    if not level_1_tag_model.load():
        raise Exception('fail to load level 1 model')
    if not level_2_tag_model.load():
        raise Exception('fail to load level 2 model')
    if not tfidf_model.load_model():
        raise Exception('fail to load tfidf model')
    return level_1_tag_model, level_2_tag_model, tfidf_model 


def _train_tag_model(level_1_tag_model, level_2_tag_model, feature_data):
    train_data = TrainData(feature_data.X, feature_data.y)
    train_X, train_y = train_data.get_train_data()
    test_X, test_y = train_data.get_test_data()
    level_1_train_y = train_y[:, 0]
    level_1_test_y = test_y[:, 0]
    logging.info('train level 1 tag model.') 
    level_1_tag_model.train(train_X, level_1_train_y)
    level_1_tag_model.evaluate(test_X, level_1_test_y)
    level_1_tag_model.save()
    level_2_train_y = train_y[:, 1]
    level_2_test_y = test_y[:, 1]
    logging.info('train level 2 tag model.') 
    level_2_tag_model.train(train_X, level_2_train_y)
    level_2_tag_model.evaluate(test_X, level_2_test_y)
    level_2_tag_model.save()
       

def train_tag_model(language):
    logging.info('train tag model. language: %s' % language)
    is_train = True
    level_1_tag_model, level_2_tag_model = get_models(language, is_train)
    feature_data_path = pm.get_tfidf_tag_feature_data_path(language)
    feature_data = FeatureData.Load(feature_data_path)
    _train_tag_model(level_1_tag_model, level_2_tag_model, feature_data)


def test_train():
    language = 'zh'
    train_tag_model(language)


def _inference_tags(tag_model, X, data_info_list):
    tag_prob_dict = {}
    tag_prob_list = tag_model.get_top_prob_tags_batch(X)
    for tag_prob, data_info in zip(tag_prob_list, data_info_list):
        _, _, _id, feature_words = data_info
        tag_prob_dict[_id] = tag_prob
    return tag_prob_dict
       

def inference_tags(level_1_tag_model, level_2_tag_model, tfidf_model, tag_data_list):
    logging.info('inference by tag model.')
    feature_data = get_tfidf_feature_data_from_tag_data_iter(tag_data_list, tfidf_model)
    X = feature_data.X
    level_1_tag_prob_list = level_1_tag_model.get_top_prob_tags_batch(X)
    level_2_tag_prob_list = level_2_tag_model.get_top_prob_tags_batch(X)
    tag_dict = {}
    for tag_data, predict_level_1_tags, predict_level_2_tags in zip(tag_data_list, 
            level_1_tag_prob_list, level_2_tag_prob_list):
        tag_data.tag_model_level_1_tags = predict_level_1_tags
        tag_data.tag_model_level_2_tags = predict_level_2_tags
        tag_dict[tag_data.item_id] = {
            'level_1': predict_level_1_tags,
            'level_2': predict_level_2_tags,
        }
    return tag_dict


def test_inference():
    language = 'zh'
    is_train = True
    level_1_tag_model, level_2_tag_model, _ = load_models(language, is_train)

    feature_data_path = pm.get_tfidf_tag_feature_data_path(language)
    feature_data = FeatureData.Load(feature_data_path)
    train_data = TrainData(feature_data.X, feature_data.y)
    test_X, test_y = train_data.get_test_data()
    test_size = 1000
    test_data = test_X[:test_size]
    test_data_info = test_y[:test_size]
    real_data_dict = {}
    for item in test_data_info:
        video_id = item[2]
        real_data_dict[video_id] = item
    predict_level_1_tag_dict = _inference_tags(level_1_tag_model, test_data, test_data_info)
    predict_level_2_tag_dict = _inference_tags(level_2_tag_model, test_data, test_data_info)

    level_1_true_count = 0
    level_2_true_count = 0
    level_1_predict_count = 0
    level_2_predict_count = 0
    for video_id, real_data_item in real_data_dict.items():
        test_data_real_level_1_tag, test_data_real_level_2_tag, _, feature_words = real_data_item
        predict_level_1_tag = predict_level_1_tag_dict[video_id]
        predict_level_2_tag = predict_level_2_tag_dict[video_id]
        
        predict_level_1_tag_set = {tag for (tag, prob) in predict_level_1_tag}
        predict_level_2_tag_set = {tag for (tag, prob) in predict_level_2_tag}
        print('video id:', video_id)
        print('feature words:', feature_words)
        print('level 1 real tag:', test_data_real_level_1_tag)
        print('level 1 predict tag:', predict_level_1_tag)
        if (test_data_real_level_1_tag in predict_level_1_tag_set
                or (test_data_real_level_1_tag == '' and len(predict_level_1_tag_set) == 0)):
            print('True')
            level_1_true_count += 1
            level_1_predict_count += 1
        elif len(predict_level_1_tag_set) == 0:
            print('Unknown')
        else:
            level_1_predict_count += 1
            print('False')
        print('level 2 real tag:', test_data_real_level_2_tag)
        print('level 2 predict tag:', predict_level_2_tag)
        if (test_data_real_level_2_tag in predict_level_2_tag_set 
                or (test_data_real_level_2_tag == '' and len(predict_level_2_tag_set) == 0)):
            print('True')
            level_2_true_count += 1
            level_2_predict_count += 1
        elif len(predict_level_2_tag_set) == 0:
            print('Unknown')
        else:
            level_2_predict_count += 1
            print('False')
    
    print('level 1 accuracy: %f, level 2 accuray: %f' % (level_1_true_count/test_size, 
            level_2_true_count/test_size))
    print('level 1 precision: %f, level 2 precision: %f' % (level_1_true_count/level_1_predict_count, 
            level_2_true_count/level_2_predict_count))


if __name__ == '__main__':
    #test_train()
    test_inference()



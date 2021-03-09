import os
import json
import argparse
import logging

from scipy.sparse import coo_matrix
import numpy as np

import kol_models.youtube_tags.commons.path_manager as pm
from kol_models.youtube_tags.commons.video_data import VideoData
from kol_models.youtube_tags.commons.tag_data import TrainTagData
from kol_models.youtube_tags.commons.text_processor import TFIDFModel
from kol_models.youtube_tags.train.tag_video_config import TagVideoConfig
from kol_models.youtube_tags.train.video_data_process import get_train_tag_data_dict_from_file

FEATURE_WORD_COUNT_MIN_THRESHOLD = 3 #4 #5 #8

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


class FeatureProcessor(object):
    def get_feature_size(self):
        return 0
    def is_valid_feature(self, feature_words):
        return True 
    def get_feature_vec(self, feature_words):
        return []


class TFIDF_FeatureProcessor(FeatureProcessor):
    def __init__(self, tfidf_model):
        self.tfidf_model = tfidf_model
    def get_feature_size(self):
        return self.tfidf_model.get_dictionary_size()
    def is_valid_feature(self, feature_words):
        is_valid = True
        if len(set(feature_words)) < FEATURE_WORD_COUNT_MIN_THRESHOLD:
            logging.info('Invalid feature, feature too small. %s' % (feature_words))
            is_valid = False
        return is_valid
    def get_feature_vec(self, feature_words):
        return self.tfidf_model.get_vector(feature_words)


def _get_feature_data_from_tag_data_iter(tag_data_iter, feature_processor):
    i = 0
    num_pos = feature_processor.get_feature_size()
    data = []
    row = []
    col = []
    tag_list = []
    for tag_data in tag_data_iter:
        feature_words = tag_data.feature_words
        item_id = tag_data.item_id
        if not feature_processor.is_valid_feature(feature_words):
            logging.info('Invalid feature, drop data! %s' % (item_id))
            continue
        feature_vec = feature_processor.get_feature_vec(feature_words)
        for pos, score in feature_vec:
            data.append(score)
            row.append(i)
            col.append(pos)
            level_1 = tag_data.level_1_tag
            level_2 = tag_data.level_2_tag
        tag_list.append((level_1, level_2, item_id, feature_words))
        i += 1
    logging.info(('process feature data finish. X data size: %d, ' 
            + 'X data shape: (%d, %d), y data size: %d') 
            % (len(data), i, num_pos, len(tag_list)))
    feature_data = FeatureData(data, row, col, i, num_pos, tag_list)
    return feature_data


def get_tfidf_feature_data_from_tag_data_iter(tag_data_iter, tfidf_model):
    tfidf_feature_processor = TFIDF_FeatureProcessor(tfidf_model)
    return _get_feature_data_from_tag_data_iter(tag_data_iter, tfidf_feature_processor)


def _get_feature_data_from_train_tag_data(language, feature_processor):
    train_tag_data_path = pm.get_train_tag_data_path(language)
    train_tag_data_dict = get_train_tag_data_dict_from_file(train_tag_data_path)
    train_tag_data_iter = train_tag_data_dict.values()
    feature_data = _get_feature_data_from_tag_data_iter(train_tag_data_iter, feature_processor)
    return feature_data


def dump_tfidf_feature_data(language):
    logging.info('dump tfidf feature data')
    tfidf_model_dir = pm.get_train_tfidf_model_dir(language)
    tfidf_model = TFIDFModel(tfidf_model_dir)        
    if not tfidf_model.load_model():
        logging.error('Failed to load tfidf model, exit!')
        return
    feature_processor = TFIDF_FeatureProcessor(tfidf_model)
    feature_data_path = pm.get_tfidf_tag_feature_data_path(language)
    feature_data = _get_feature_data_from_train_tag_data(language, feature_processor)
    feature_data.save(feature_data_path)


def main(args):
    #print(args) 
    language = args['lang']
    feature_type = args['type']
    logging.info('feature data process. language: %s, feature_type %s' 
            % (language, feature_type))
    if feature_type == 'tfidf':
        dump_tfidf_feature_data(language)
    else:
        logging.error('invalid feature type!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--lang',
        choices={
            'en',
            'zh',
            'ko',
            'ja',
        },
        required=True,
        help='language',
    ) 
    parser.add_argument(
        '--type',
        choices={
            'tfidf',
        },
        default='tfidf',
        help='feature data type',
    ) 
    args = parser.parse_args()
    arguments = args.__dict__
    main(arguments)



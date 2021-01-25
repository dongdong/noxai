import json
import numpy as np
import logging
import math
import random
from collections import Counter
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MultiLabelBinarizer

class GroupStatModel:

    not_support_group_stat = Counter()

    def __init__(self, group_min_size):
        self.age_gender_dist_dict = None #age_gender_dist_dict
        self.group_size_min_threshold = group_min_size #3 #5 #8

    def _add_group(self, group_list, group_name, group_value, group_dict):
        if group_value == '':
            return
        group_key = '%s##%s' % (group_name, group_value)
        if group_dict is None or group_key in group_dict:
            group_list.append(group_key)
        else:
            logging.info('not support group: %s' % group_key)
            GroupStatModel.not_support_group_stat.update([group_key])

    def _add_cross_group(self, group_list, group_1_name, group_1_value, 
                group_2_name, group_2_value, group_dict):
        if group_1_value == '' or group_2_value == '':
            return
        group_key = '%s##%s&&%s##%s' % (group_1_name, group_1_value, 
                group_2_name, group_2_value)
        if group_dict is None or group_key in group_dict:
            group_list.append(group_key)
 
    def _get_channel_groups(self, channel_data, group_dict=None):
        groups = []
        country_value = channel_data.country_code.lower()
        language_value = channel_data.language.lower()
        #self._add_group(groups, 'country', country_value, group_dict)
        #self._add_group(groups, 'language', language_value, group_dict)
        #self._add_cross_group(groups, 'country', country_value, 
        #        'language', language_value, group_dict)
        for tag_id in channel_data.tag_id_list:
            self._add_group(groups, 'tag_id', tag_id, group_dict)
            self._add_cross_group(groups, 'tag_id', tag_id, 
                    'country', country_value, group_dict)
            self._add_cross_group(groups, 'tag_id', tag_id,
                    'language', language_value, group_dict)
            for tag_id_1 in channel_data.tag_id_list:
                self._add_cross_group(groups, 'tag_id', tag_id,
                        'tag_id', tag_id_1, group_dict)
            for category in channel_data.category_list:
                self._add_cross_group(groups, 'tag_id', tag_id,
                        'category', category, group_dict)
        for tag_name in channel_data.tag_name_list:
            tag_name = tag_name.lower()
            self._add_group(groups, 'tag_name', tag_name, group_dict)
            #self._add_cross_group(groups, 'tag_name', tag_name,
            #        'country', country_value, group_dict)
            #self._add_cross_group(groups, 'tag_name', tag_name,
            #        'language', language_value, group_dict)
        for category in channel_data.category_list:
            category_value = str(category).lower()
            self._add_group(groups, 'category', category_value, group_dict)
            self._add_cross_group(groups, 'category', category_value, 
                    'country', country_value, group_dict)
            self._add_cross_group(groups, 'category', category_value, 
                    'language', language_value, group_dict)
        groups.append('all')
        return groups

    def _group_channel_data(self, channel_data_dict):
        group_dist_dict = {}
        for channel_id, channel_data in channel_data_dict.items():
            groups = self._get_channel_groups(channel_data)
            for group in groups:
                if group not in group_dist_dict:
                    group_dist_dict[group] = []
                group_dist_dict[group].append(channel_data.age_gender_dist)

        group_dist_dict = {group:channel_list 
                for group, channel_list in group_dist_dict.items()
                if len(channel_list) >= self.group_size_min_threshold}
        return group_dist_dict

    def _average_dist_list(self, dist_list):
        dist_arr = np.array(dist_list)
        avg_dist = np.mean(dist_arr, axis=0)
        total = np.sum(avg_dist)
        normalized_dist = avg_dist / total * 100
        normalized_dist_list = normalized_dist.tolist()
        return normalized_dist_list

    def train(self, channel_data_dict):
        self.age_gender_dist_dict = {}
        channel_group_dict = self._group_channel_data(channel_data_dict)
        for group, dist_list in channel_group_dict.items():
            group_age_gender_dist = self._average_dist_list(dist_list)
            self.age_gender_dist_dict[group] = group_age_gender_dist
            #logging.info('training... group: %s, age gender dist: %s' 
            #        % (group, group_age_gender_dist))       

    def _adjust_dist(self, dist_list):
        all_dist = self.age_gender_dist_dict['all']
        new_dist = [v1 * v1 / v2 for v1, v2 in zip(dist_list, all_dist)]
        total_value = sum(new_dist)
        normalized_new_dist = [v / total_value * 100 for v in new_dist]
        return normalized_new_dist

    def inference(self, channel_data):
        assert self.age_gender_dist_dict is not None
        assert channel_data is not None
       
        groups = self._get_channel_groups(channel_data, self.age_gender_dist_dict)
        logging.info('GroupStatModel. channel_id: %s, valid stat groups: %s' % 
                (channel_data.channel_id, str(groups)))
        dist_list = []
        for group in groups:
            group_age_gender_dist = self.age_gender_dist_dict[group]
            new_dist = self._adjust_dist(group_age_gender_dist)
            dist_list.append(new_dist)#(group_age_gender_dist)
                
        age_gender_dist = self._average_dist_list(dist_list)
        return age_gender_dist
 

class KNN_Model:   
    def __init__(self, n_neighbors):
        self.model_list = None
        self.feature_transformer = None
        self.n_neighbors = n_neighbors
        #self.n_neighbors = 5 #8

    def _get_item_features(self, channel_data):
        feature_list = []
        language = channel_data.language.lower()
        country_code = channel_data.country_code.lower()
        for tag_id in channel_data.tag_id_list:
            tag_id_value = 'tag_id##%s' % tag_id
            feature_list.append(tag_id_value)
            if language != '':
                tag_language_value = 'tag_id##%s##%s' % (tag_id, language)
                feature_list.append(tag_language_value)
            if country_code != '':
                tag_country_value = 'tag_id##%s##%s' % (tag_id, country_code)
                feature_list.append(tag_country_value)
            for category in channel_data.category_list:
                category = str(category).lower() # few channels have categories of type int 
                tag_category_value = 'tag_id##%s##%s' % (tag_id, category)
                feature_list.append(tag_category_value)
            for tag_id_1 in channel_data.tag_id_list:
                tag_id_pair_value = 'tag_id_pair##%s##%s' % (tag_id, tag_id_1)
                feature_list.append(tag_id_pair_value) 
        for tag_name in channel_data.tag_name_list:
            tag_name_value = 'tag_name##%s' % tag_name.lower()
            feature_list.append(tag_name_value)
        return feature_list

    def _process_data(self, channel_data_iter, is_train=True):
        feature_class_counter = Counter()
        feature_list = []
        value_list = []
        for channel_data in channel_data_iter:
            item_feature_list = self._get_item_features(channel_data)
            feature_list.append(item_feature_list)
            if is_train:
                feature_class_counter.update(item_feature_list)
                value_list.append(channel_data.age_gender_dist)

        if is_train:
            self.feature_transformer = MultiLabelBinarizer()
            X = self.feature_transformer.fit_transform(feature_list)
            Y = np.array(value_list) 
            logging.info('process train data finish. X shape: %s, Y shape: %s' 
                    % (str(X.shape), str(Y.shape)))
        else:
            logging.info('feature list: %s' % feature_list)
            X = self.feature_transformer.transform(feature_list)
            Y = None
            logging.info('process inference data finish. X shape: %s' 
                    % (str(X.shape)))
        return X, Y

    def train(self, channel_data_dict):
        X, Y = self._process_data(channel_data_dict.values())
        num_models = Y.shape[1]
        self.model_list = []
        for i in range(num_models):
            model = KNeighborsRegressor(n_neighbors=self.n_neighbors) 
            y = Y[:, i]
            model.fit(X, y)
            self.model_list.append(model) 
 
    def inference(self, channel_data):
        assert self.model_list is not None
        assert self.feature_transformer is not None

        X, _ = self._process_data([channel_data], False)
        y_pred = [model.predict(X)[0] for model in self.model_list]
        total = sum(y_pred)
        y_pred_norm = [v / total * 100 for v in y_pred]
        #print(total, sum(y_pred_norm))
        return y_pred_norm


class EnsembleModel:
    def __init__(self):
        self.model_list = [
            #GroupStatModel(1),
            GroupStatModel(2),
            GroupStatModel(4),
            GroupStatModel(8),
            GroupStatModel(16),
            KNN_Model(5),
            #KNN_Model(8),
        ]

    def train(self, channel_data_dict):
        for model in self.model_list:
            model.train(channel_data_dict)

    def inference(self, channel_data):
        sum_list = None
        for model in self.model_list:
            pred_list = model.inference(channel_data)
            logging.info('EnsembleModel, predict result: %s' % (str(pred_list)))
            if sum_list is None:
                sum_list = [v for v in pred_list]
            else:
                sum_list = [v1 + v2 for v1, v2 in zip(sum_list, pred_list)]
        # random
        #sum_list = [random.uniform(0.95, 1.05) * v for v in sum_list]
        #sum_list = [random.uniform(0.9, 1.1) * v for v in sum_list]
        sum_list = [random.uniform(0.85, 1.15) * v for v in sum_list]
        total = sum(sum_list)
        norm_list = [v / total * 100 for v in sum_list]
        return norm_list


def test_train():
    from fans_dist import load_true_dist_channels, true_dist_cache_path
    channel_data_dict = load_true_dist_channels(true_dist_cache_path)
    #model = GroupStatModel()
    #model.train(channel_data_dict) 

    model = KNN_Model()
    model.train(channel_data_dict)

    test_list = list(channel_data_dict.values())[:10]

    for i in range(len(test_list)):
        channel_data = test_list[i]
        print(channel_data.channel_id)
        print(channel_data.age_gender_dist)
        y = model.inference(channel_data)
        y = [round(v, 1) for v in y]
        print(y)


def test_inference():
    from channel_data import ChannelData
    channel_id = 'UC--70ql_IxJmhmqXqrkJrWQ' 
    model = GroupStatModel.Load(group_stat_model_path)
    channel_data = ChannelData.From_es(channel_id)
    age_gender_dist = model.inference(channel_data)
    print(age_gender_dist)


if __name__ == '__main__':
    test_train()
    #test_inference()


import json
import numpy as np
import logging
import math
from collections import Counter
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MultiLabelBinarizer


class KNN_Model:   
    def __init__(self):
        self.model_list = None
        self.feature_transformer = None
        self.n_neighbors = 8

    def _get_item_features(self, channel_data):
        feature_list = []
        language = channel_data.language.lower()
        #language_value = 'language##%s' % language
        #feature_list.append(language_value)
        country_code = channel_data.country_code.lower()
        if country_code != '':
            #country_value = 'country##%s' % country_code
            #feature_list.append(country_value)
            country_language_value = 'country##%s##%s' % (country_code, language)
            feature_list.append(country_language_value)
        for category in channel_data.category_list:
            category = category.lower()
            category_value = 'category##%s' % category
            feature_list.append(category_value)
            category_language_value = 'category##%s##%s' % (category, language)
            feature_list.append(category_language_value)
            if country_code != '':
                category_country_value = 'category##%s##%s' % (category, country_code)
                feature_list.append(category_country_value)
        for tag_id in channel_data.tag_id_list:
            tag_id_value = 'tag_id##%s' % tag_id
            feature_list.append(tag_id_value)
            tag_language_value = 'tag_id##%s##%s' % (tag_id, language)
            feature_list.append(tag_language_value)
            if country_code != '':
                tag_country_value = 'tag_id##%s##%s' % (tag_id, country_code)
                feature_list.append(tag_country_value)
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
    

    def save(self, file_path):
        pass

    @classmethod
    def Load(cls, file_path):
        pass


def test_train():
    from fans_dist import load_true_dist_channels, true_dist_cache_path
    channel_data_dict = load_true_dist_channels(true_dist_cache_path)
    #model = GroupStatModel()
    #model.train(channel_data_dict) 
    #model.save(group_stat_model_path)

    #model = CrossGroupStatModel()
    #model.train(channel_data_dict) 
    #model.save(cross_group_stat_model_path)

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
    from fans_dist import ChannelData
    channel_id = 'UC--70ql_IxJmhmqXqrkJrWQ' 
    model = GroupStatModel.Load(group_stat_model_path)
    channel_data = ChannelData.From_es(channel_id)
    age_gender_dist = model.inference(channel_data)
    print(age_gender_dist)


if __name__ == '__main__':
    test_train()
    #test_inference()


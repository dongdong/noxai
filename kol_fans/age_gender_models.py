import json
import numpy as np
import logging
import math
from collections import Counter

group_stat_model_path = './group_stat_model.json'
cross_group_stat_model_path = './cross_group_stat_model.json'

class GroupStatModel:

    not_support_group_stat = Counter()

    def __init__(self, age_gender_dist_dict=None):
        self.age_gender_dist_dict = age_gender_dist_dict
        self.group_size_min_threshold = 5 #8

    def _add_group(self, group_list, group_name, group_value, group_dict):
        if group_value == '':
            return
        group_key = '%s##%s' % (group_name, group_value)
        if group_dict is None or group_key in group_dict:
            group_list.append(group_key)
        else:
            logging.info('not support group: %s' % group_key)
            GroupStatModel.not_support_group_stat.update([group_key])

    def _get_channel_groups(self, channel_data, group_dict=None):
        groups = []
        country_value = channel_data.country_code.lower()
        self._add_group(groups, 'country', country_value, group_dict)
        language_value = channel_data.language.lower()
        self._add_group(groups, 'language', language_value, group_dict)
        for category in channel_data.category_list:
            self._add_group(groups, 'category', category.lower(), group_dict)
        for tag_id in channel_data.tag_id_list:
            self._add_group(groups, 'tag_id', tag_id, group_dict)
        for tag_name in channel_data.tag_name_list:
            self._add_group(groups, 'tag_name', tag_name.lower(), group_dict)
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
            logging.info('training... group: %s, age gender dist: %s' 
                    % (group, group_age_gender_dist))       

    def inference(self, channel_data):
        assert self.age_gender_dist_dict is not None
        assert channel_data is not None
       
        groups = self._get_channel_groups(channel_data, self.age_gender_dist_dict)
        logging.info('GroupStatModel. channel_id: %s, valid stat groups: %s' % 
                (channel_data.channel_id, str(groups)))
        dist_list = []
        for group in groups:
            group_age_gender_dist = self.age_gender_dist_dict[group]
            dist_list.append(group_age_gender_dist)
                
        age_gender_dist = self._average_dist_list(dist_list)
        return age_gender_dist
    
    def save(self, file_path):
        succ = False
        if self.age_gender_dist_dict is not None:
            with open(file_path, 'w') as f:
                json.dump(self.age_gender_dist_dict, f)
                succ = True
        return succ

    @classmethod
    def Load(cls, file_path):
        model = None
        with open(file_path, 'r') as f:
            json_obj = json.load(f)
            #model = GroupStatModel(json_obj)
            model = cls(json_obj)
        return model
    

class WeightedGroupStatModel(GroupStatModel):
    def __init__(self, age_gender_dist_dict=None):
        super(WeightedGroupStatModel, self).__init__(age_gender_dist_dict)

    def inference(self, channel_data):
        assert self.age_gender_dist_dict is not None
        assert channel_data is not None
       
        group_weight_map = {
            'country': 1,
            'language': 1,
            'category': 2,
            'tag_id': 3,
            'tag_name': 2,
        } 
        groups = self._get_channel_groups(channel_data, self.age_gender_dist_dict)
        logging.info('WeightedGroupStatModel. channel_id: %s, valid stat groups: %s' % 
                (channel_data.channel_id, str(groups)))
        dist_list = []
        for group in groups:
            group_age_gender_dist = self.age_gender_dist_dict[group]
            group_name = group.split('##')[0]
            if group_name not in group_weight_map:
                continue
            group_weight = group_weight_map[group_name]
            for i in range(1, group_weight + 1):
                value_list = [math.pow(value, i) for value in group_age_gender_dist]
                value_list = [value / sum(value_list) * 100 for value in value_list]
                dist_list.append(value_list)
                #print(group, value_list)
            ''' 
            value_list = [math.pow(value, group_weight) for value in group_age_gender_dist]
            value_list = [value / sum(value_list) * 100 for value in value_list]
            dist_list.append(value_list)
            print(group, value_list)
            '''
        age_gender_dist = self._average_dist_list(dist_list)
        return age_gender_dist
 


class CrossGroupStatModel(GroupStatModel):
    def __init__(self, age_gender_dist_dict=None):
        super(CrossGroupStatModel, self).__init__(age_gender_dist_dict)

    def _add_cross_group(self, group_list, group_1_name, group_1_value, 
                group_2_name, group_2_value, group_dict):
        if group_1_value == '' or group_2_value == '':
            return
        group_key = '%s##%s&&%s##%s' % (group_1_name, group_1_value, 
                group_2_name, group_2_value)
        if group_dict is None or group_key in group_dict:
            group_list.append(group_key)
        #else:
        #    logging.info('not support group: %s' % group_key)

    def _get_channel_groups(self, channel_data, group_dict=None):
        groups = []
        country_value = channel_data.country_code.lower()
        language_value = channel_data.language.lower()
        #self._add_group(groups, 'country', country_value, group_dict)
        #self._add_group(groups, 'language', language_value, group_dict)
        for category in channel_data.category_list:
            category_value = category.lower()
            self._add_group(groups, 'category', category_value, group_dict)
            self._add_cross_group(groups, 'category', category_value, 
                    'country', country_value, group_dict)
            self._add_cross_group(groups, 'category', category_value, 
                    'language', language_value, group_dict)
        for tag_id in channel_data.tag_id_list:
            self._add_group(groups, 'tag_id', tag_id, group_dict)
            self._add_cross_group(groups, 'tag_id', tag_id, 
                    'country', country_value, group_dict)
            self._add_cross_group(groups, 'tag_id', tag_id,
                    'language', language_value, group_dict)
        for tag_name in channel_data.tag_name_list:
            tag_name = tag_name.lower()
            self._add_group(groups, 'tag_name', tag_name, group_dict)
            self._add_cross_group(groups, 'tag_name', tag_name,
                    'country', country_value, group_dict)
            self._add_cross_group(groups, 'tag_name', tag_name,
                    'language', language_value, group_dict)
        groups.append('all')
        return groups
    

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
 

    


def train():
    from fans_dist import load_true_dist_channels, true_dist_cache_path
    channel_data_dict = load_true_dist_channels(true_dist_cache_path)
    #model = GroupStatModel()
    #model.train(channel_data_dict) 
    #model.save(group_stat_model_path)

    model = CrossGroupStatModel()
    model.train(channel_data_dict) 
    model.save(cross_group_stat_model_path)

def test_inference():
    from fans_dist import ChannelData
    channel_id = 'UC--70ql_IxJmhmqXqrkJrWQ' 
    model = GroupStatModel.Load(group_stat_model_path)
    channel_data = ChannelData.From_es(channel_id)
    age_gender_dist = model.inference(channel_data)
    print(age_gender_dist)


if __name__ == '__main__':
    #train()
    test_inference()


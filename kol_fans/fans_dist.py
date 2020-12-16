from collections import Counter
import json
import re
import time
import numpy as np

import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

from utils import get_rows_by_sql_from_kol_v2
from utils import get_channel_contents_from_es, update_channel_contents_to_es
from utils import pop_channel_id_from_pipe
from age_gender_models import KNN_Model

true_dist_cache_path = './true_dist_channel_data.json'

age_gender_dist_config = [
    'age13-17##male',
    'age13-17##female',
    'age18-24##male',
    'age18-24##female',
    'age25-34##male',
    'age25-34##female',
    'age35-44##male',
    'age35-44##female',
    'age45-54##male',
    'age45-54##female',
    'age55-64##male',
    'age55-64##female',
    'age65+##male',
    'age65+##female',
]

age_gender_dist_order_dict = {key:i for i, key in enumerate(age_gender_dist_config)}


class ChannelData():
    def __init__(self, channel_id, country_code, language, category_list, 
            tag_id_list, tag_name_list):
        self.channel_id = channel_id
        self.country_code = country_code
        self.language = language
        self.category_list = category_list
        self.tag_id_list = tag_id_list
        self.tag_name_list = tag_name_list
        self.age_gender_dist = None
    
    def set_age_gender_dist(self, age_gender_dist):
        self.age_gender_dist = age_gender_dist

    def get_data(self):
        data = {
            'channel_id': self.channel_id,
            'country_code': self.country_code,
            'language': self.language,
            'categories': self.category_list,
            'tag_ids': self.tag_id_list,
            'tag_names': self.tag_name_list,
        }
        if self.age_gender_dist is not None:
            data['age_gender_dist'] = self.age_gender_dist
        return data

    def get_follower_age_gender_distribution(self):
        if self.age_gender_dist is None:
            return None
        format_dist_list = []
        for key, value in zip(age_gender_dist_config, self.age_gender_dist):
            age, gender = key.split('##')
            age = age[3:]
            item = {
                'gender': gender,
                'age': age,
                'value': round(value, 1)
            }
            format_dist_list.append(item)
        return format_dist_list

    @staticmethod
    def From_es(channel_id):
        channel_data = None
        try:
            channel_contents = get_channel_contents_from_es(channel_id)
            country_code = channel_contents.get('country_code', '')
            # @TODO
            if country_code == 'OTHERS':
                country_code = ''
            language = channel_contents.get('languages', '')
            category_list = channel_contents.get('category', [])
            tag_id_list = [ tag['id'] for tag in channel_contents.get('tag_detail', [])]
            tag_name_list = channel_contents.get('tag_list', [])
            channel_data = ChannelData(channel_id, country_code, language, category_list, 
                    tag_id_list, tag_name_list)
            age_gender_dist = channel_contents.get('follower_age_gender_distribution', None)
            #if age_gender_dist is not None:
            #    logging.info('get channel data. follower age gender distribution: %s' 
            #            % age_gender_dist)
            #else:
            #    logging.info('get channel data. follower age gender distribution NOT set') 
        except:
            logging.warn('Fail to get channel data. channel_id: %s' % (channel_id))
        return channel_data
    
    @staticmethod
    def From_dict(json_obj):
        channel_data = None
        try:
            channel_id = json_obj['channel_id']
            country_code = json_obj['country_code']
            language = json_obj['language']
            category_list = json_obj['categories']
            tag_id_list = json_obj['tag_ids']
            tag_name_list = json_obj['tag_names']
            channel_data = ChannelData(channel_id, country_code, language, category_list, 
                    tag_id_list, tag_name_list)
            age_gender_dist = json_obj.get('age_gender_dist', None)
            if age_gender_dist is not None:
                channel_data.set_age_gender_dist(age_gender_dist)
        except:
            logging.warn('Fail to get channel data. contents: %s' % str(json_obj))
            #logging.error(traceback.format_exc())
        return channel_data


def get_age_gender_dist_arr(raw_list):
    dist_arr = [0.0 for i in range(len(age_gender_dist_config))]
    for item in raw_list:
        age, gender, dist_score = item
        if age == 'age65-':
            age = 'age65+'
        key = '%s##%s' % (age, gender)
        index = age_gender_dist_order_dict[key]
        dist_arr[index] = dist_score
    return dist_arr


def get_true_fans_distribution():
    data_list = []
    sql = 'select * from ytb_analytics_data_origin_real_data'
    rows = get_rows_by_sql_from_kol_v2(sql)
    for row in rows:
        data = {
            'channel_id': row[1],
            'country_code': row[3],
            'category_id': row[4],
        }
        try:
            age_gender_dist_raw = json.loads(row[5])
            raw_list = age_gender_dist_raw['rows']
            age_gender_dist_arr = get_age_gender_dist_arr(raw_list)
            data['age_gender_dist'] = age_gender_dist_arr
            if (sum(age_gender_dist_arr) < 0.1 or # all zeros
                sum(age_gender_dist_arr) > 120 or # not sum upto 100, maybe fake data
                max(age_gender_dist_arr) > 70 ): # extreme distribution
                logging.warn('get true fans distribution. Invalid data: %s' % (row[5]))
                continue
            data_list.append(data)
        except:
            logging.warn('data err! dist info miss: %s' % row[5])
    return data_list


def dump_true_dist_channels(file_path):
    true_dist_list = get_true_fans_distribution()
    logging.info('get true fans distribution, total: %d' % len(true_dist_list))
    count = 0
    with open(file_path, 'w') as f:
        for item in true_dist_list:
            channel_id = item['channel_id']
            age_gender_dist = item['age_gender_dist']

            channel_data = ChannelData.From_es(channel_id)
            if channel_data is None:
                continue
            channel_data.set_age_gender_dist(age_gender_dist)
            f.write(json.dumps(channel_data.get_data()) + '\n')
            count += 1
    logging.info('dump true dist channels finish. total data: %d' % count)


def load_true_dist_channels(file_path):
    channel_data_dict = {} 
    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            channel_data = ChannelData.From_dict(data)
            channel_data_dict[channel_data.channel_id] = channel_data
    logging.info('load true dist channels finish. total: %d' % len(channel_data_dict))
    return channel_data_dict


def inference_age_gender_dist(model, channel_id):
    logging.info('inference age gender dist. channel_id: %s' % (channel_id))
    channel_data = ChannelData.From_es(channel_id)
    if channel_data is not None:
        age_gender_dist = model.inference(channel_data)
        channel_data.set_age_gender_dist(age_gender_dist)
    return channel_data


global_true_dist_channel_data_dict = None
global_age_gender_dist_model = None
def load_age_gender_model():
    global global_true_dist_channel_data_dict
    global global_age_gender_dist_model
    if global_true_dist_channel_data_dict is None:
        global_true_dist_channel_data_dict = load_true_dist_channels(true_dist_cache_path)
    if global_age_gender_dist_model is None:
        global_age_gender_dist_model = KNN_Model()
        global_age_gender_dist_model.train(global_true_dist_channel_data_dict)
    return global_true_dist_channel_data_dict, global_age_gender_dist_model 


def get_age_gender_distribution(channel_id):
    true_dist_channel_data_dict, age_gender_dist_model = load_age_gender_model()
    if channel_id in true_dist_channel_data_dict:
        logging.info('True age gender distribution, ignore! channel id: %s'% (channel_id))
        channel_data = true_dist_channel_data_dict[channel_id]
    else:
        channel_data = inference_age_gender_dist(age_gender_dist_model, channel_id)
    format_age_gender_dist = channel_data.get_follower_age_gender_distribution()
    logging.info('predict follower age gender distribution: \n%s' % format_age_gender_dist)
    return format_age_gender_dist   


def test_age_gender_inference():
    true_dist_channel_data_dict, age_gender_dist_model = load_age_gender_model()
    for channel_id, channel_data in true_dist_channel_data_dict.items():
        channel_data_infer = inference_age_gender_dist(age_gender_dist_model, channel_id)    
        logging.info('real dist: %s' % (channel_data.age_gender_dist))
        logging.info('infer dist: %s' % (channel_data_infer.age_gender_dist))
        #logging.info('real dist argsort: %s' 
        #        % (np.array(channel_data.age_gender_dist).argsort()))
        #logging.info('infer dist argsort: %s' 
        #        % (np.array(channel_data_infer.age_gender_dist).argsort()))


def dump_real_data():
    dump_true_dist_channels(true_dist_cache_path)


def display_channel_contents(channel_id):
    channel_contents = get_channel_contents_from_es(channel_id)
    for k, v in channel_contents.items():
        print(k, v)


def update_age_gender_dist_to_es(channel_id, age_gender_dist):
    data = {
        "follower_age_gender_distribution": age_gender_dist,
    }
    update_channel_contents_to_es(channel_id, data) 


def update_age_gender_dist(channel_id):
    logging.info('update followers age gender dist. channel id: %s' % (channel_id))
    age_gender_dist = get_age_gender_distribution(channel_id)
    update_age_gender_dist_to_es(channel_id, age_gender_dist)


def update_age_gender_dist_from_pipe():
    count = 0
    batch = 10

    for channel_id in pop_channel_id_from_pipe():
        update_age_gender_dist(channel_id)
        count += 1
        if count % batch == 0:
            time.sleep(1)

    logging.info('predict age gender distribution finish! total: %d' % (count))


def test():
    #dump_true_dist_channels(true_dist_cache_path)
    test_age_gender_inference()   


if __name__ == '__main__':
    test()

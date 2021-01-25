import os
import json
import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

from kol_models.commons.mysql_utils import get_origin_real_data_iter
from kol_models.youtube_fans.commons import ChannelData, get_age_gender_dist_arr

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_data_dir = os.path.join(_cur_dir, 'data')
_real_dist_data_path = os.path.join(_data_dir, 'real_dist_channel_data.json')


def _is_valid_age_gender_dist(age_gender_dist_arr):
    if (sum(age_gender_dist_arr) < 0.1 or # all zeros
        sum(age_gender_dist_arr) > 120 or # not sum upto 100, maybe fake data
        max(age_gender_dist_arr) > 70 ): # extreme distribution
        return False
    else:
        return True


def _get_real_distribution_data():
    logging.info('get real distribution data from mysql')
    data_list = []
    real_data_iter = get_origin_real_data_iter()
    for row in real_data_iter:
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
            if not _is_valid_age_gender_dist:
                logging.warn('get real fans distribution. Invalid data: %s' % (row[5]))
                continue
            data_list.append(data)
        except:
            logging.warn('data err! dist info miss: %s' % row[5])
    return data_list


def _dump_real_dist_channels(file_path):
    logging.info('dump real dist channels to file. path: %s' % (file_path))
    real_dist_list = _get_real_distribution_data()
    logging.info('get real fans distribution, total: %d' % len(real_dist_list))
    count = 0
    with open(file_path, 'w') as f:
        for item in real_dist_list:
            channel_id = item['channel_id']
            age_gender_dist = item['age_gender_dist']
            channel_data = ChannelData.From_es(channel_id)
            if channel_data is None:
                logging.warn('Fail to load channel data from ES. channel id: %s' % (channel_id))
                continue
            channel_data.set_age_gender_dist(age_gender_dist)
            f.write(json.dumps(channel_data.get_data()) + '\n')
            count += 1
    logging.info('dump real dist channels finish. total data: %d' % count)


def _load_real_dist_channels(file_path):
    logging.info('load real dist channels from file. path: %s' % (file_path))
    channel_data_dict = {} 
    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            channel_data = ChannelData.From_dict(data)
            channel_data_dict[channel_data.channel_id] = channel_data
    logging.info('load real dist channels finish. total: %d' % len(channel_data_dict))
    return channel_data_dict


def dump_train_data():
    _dump_real_dist_channels(_real_dist_data_path)    


def load_train_data():
    channel_data_dict = _load_real_dist_channels(_real_dist_data_path)
    return channel_data_dict


def test():
    dump_train_data()
    load_train_data()


if __name__ == '__main__':
    test()


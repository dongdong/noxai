import logging
from kol_models.commons.es_utils import get_channel_contents as get_channel_contents_from_es 


_age_gender_dist_config = [
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

_age_gender_dist_order_dict = {key:i for i, key in enumerate(_age_gender_dist_config)}


def get_age_gender_dist_arr(raw_list):
    dist_arr = [0.0 for i in range(len(_age_gender_dist_config))]
    for item in raw_list:
        age, gender, dist_score = item
        if age == 'age65-':
            age = 'age65+'
        key = '%s##%s' % (age, gender)
        index = _age_gender_dist_order_dict[key]
        dist_arr[index] = dist_score
    return dist_arr


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

    def get_follower_age_gender_distribution(self):
        if self.age_gender_dist is None:
            return None
        format_dist_list = []
        for key, value in zip(_age_gender_dist_config, self.age_gender_dist):
            age, gender = key.split('##')
            age = age[3:]
            item = {
                'gender': gender,
                'age': age,
                'value': round(value, 1)
            }
            format_dist_list.append(item)
        return format_dist_list

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
            if language is None:
                language = ''
            category_list = channel_contents.get('category', [])
            tag_id_list = [ tag['id'] for tag in channel_contents.get('tag_detail', [])]
            tag_name_list = channel_contents.get('tag_list', [])
            channel_data = ChannelData(channel_id, country_code, language, category_list, 
                    tag_id_list, tag_name_list)
        except:
            logging.warn('Fail to get channel data. channel_id: %s' % (channel_id))
            #logging.error(traceback.format_exc())
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


def test_channel():
    channel_id = 'UCRs1pHnES3QDdh43xbjOmzw'
    channel_data = ChannelData.From_es(channel_id)
    print(channel_data.get_data())


if __name__ == '__main__':
    test_channel()



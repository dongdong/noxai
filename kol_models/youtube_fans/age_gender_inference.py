import json

import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

from kol_models.youtube_fans.commons import ChannelData
from kol_models.youtube_fans.age_gender_models import GroupStatModel, KNN_Model, EnsembleModel
from kol_models.youtube_fans.train_data import load_train_data
from kol_models.commons.es_utils import update_channel_contents 

_global_real_dist_channel_data_dict = None
_global_age_gender_dist_model = None
def _load_age_gender_model():
    global _global_real_dist_channel_data_dict
    global _global_age_gender_dist_model
    if _global_real_dist_channel_data_dict is None:
        _global_real_dist_channel_data_dict = load_train_data()
    if _global_age_gender_dist_model is None:
        #_global_age_gender_dist_model = KNN_Model()
        #_global_age_gender_dist_model = GroupStatModel()
        _global_age_gender_dist_model = EnsembleModel()
        _global_age_gender_dist_model.train(_global_real_dist_channel_data_dict)
    return _global_real_dist_channel_data_dict, _global_age_gender_dist_model 


def _inference_age_gender_dist(model, channel_id):
    logging.info('inference age gender dist. channel_id: %s' % (channel_id))
    channel_data = ChannelData.From_es(channel_id)
    if channel_data is not None:
        age_gender_dist = model.inference(channel_data)
        channel_data.set_age_gender_dist(age_gender_dist)
    return channel_data


def _update_age_gender_dist_to_es(channel_id, age_gender_dist):
    data = {
        "follower_age_gender_distribution": age_gender_dist,
    }
    update_channel_contents(channel_id, data) 


def get_age_gender_distribution(channel_id):
    real_dist_channel_data_dict, age_gender_dist_model = _load_age_gender_model()
    if channel_id in real_dist_channel_data_dict:
        logging.info('Real age gender distribution, ignore! channel id: %s'% (channel_id))
        channel_data = real_dist_channel_data_dict[channel_id]
    else:
        channel_data = _inference_age_gender_dist(age_gender_dist_model, channel_id)
    if channel_data is not None:
        format_age_gender_dist = channel_data.get_follower_age_gender_distribution()
        logging.info('predict follower age gender distribution: \n%s' % format_age_gender_dist)
    else:
        format_age_gender_dist = None
    return format_age_gender_dist   


def test_age_gender_inference():
    real_dist_channel_data_dict, age_gender_dist_model = _load_age_gender_model()
    for channel_id, channel_data in real_dist_channel_data_dict.items():
        channel_data_infer = _inference_age_gender_dist(age_gender_dist_model, channel_id)    
        logging.info('real dist: %s' % (channel_data.age_gender_dist))
        logging.info('infer dist: %s' % (channel_data_infer.age_gender_dist))


def update_age_gender_dist(channel_id):
    logging.info('update age gender dist. channel id: %s' % (channel_id))
    age_gender_dist = get_age_gender_distribution(channel_id)
    if age_gender_dist is not None:
        _update_age_gender_dist_to_es(channel_id, age_gender_dist)


def test():
    test_age_gender_inference()   


if __name__ == '__main__':
    test()

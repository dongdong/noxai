import os
import logging
import traceback

from kol_models.youtube_tags.commons.channel_data import ChannelData
from kol_models.youtube_tags.models.tag_model import TagModel
from kol_models.youtube_tags.models.tag_model import load_models as load_tag_models
from kol_models.youtube_tags.models.tag_model import inference_tags as inference_model_tags
from kol_models.youtube_tags.models.tag_match_model import inference_tags as inference_match_model_tags
from kol_models.youtube_tags.models.default_match_model import inference_tags as inference_default_match_tags
from kol_models.youtube_tags.models.youtube_topic_tags import inference_tags as inference_ytb_tags


_global_tag_model_dict = None

def _load_tag_models():
    global _global_tag_model_dict 
    _global_tag_model_dict = {}
    lang_list = ['en', 'zh', 'ko', 'ja']
    for lang in lang_list:
        try:
            level_1_model, level_2_model, tfidf_model = load_tag_models(lang, is_train=False)
            _global_tag_model_dict[lang] = (level_1_model, level_2_model, tfidf_model)
        except Exception as e:
            logging.error('load model error! language: %s. detail: %s' % (lang, str(e)))


def _get_tag_model(lang):
    level_1_tag_model, level_2_tag_model, tfidf_model = None, None, None
    if _global_tag_model_dict is None:
        _load_tag_models()
    if lang in _global_tag_model_dict:
        level_1_tag_model, level_2_tag_model, tfidf_model = _global_tag_model_dict[lang]
    return level_1_tag_model, level_2_tag_model, tfidf_model


def _inference_model_tags(language, tag_data_list):
    level_1_tag_model, level_2_tag_model, tfidf_model = _get_tag_model(language)
    if level_1_tag_model is not None and level_2_tag_model is not None and tfidf_model is not None:
        inference_model_tags(level_1_tag_model, level_2_tag_model, tfidf_model, tag_data_list)

def _inference_ytb_tags(language, tag_data_list):
    inference_ytb_tags(language, tag_data_list)


def _inference_match_model_tags(language, tag_data_list):
    lang_set = set(['en', 'zh', 'ko', 'ja'])
    if language in lang_set:
        inference_match_model_tags(tag_data_list)
    else:
        inference_default_match_tags(tag_data_list)


def inference_tags(language, tag_data_list):
    lang_map = {
        'zh-Hant': 'zh',
        'zh-Hans': 'zh',
    }
    if language in lang_map:
        language = lang_map[language]
    _inference_model_tags(language, tag_data_list)
    _inference_ytb_tags(language, tag_data_list)
    _inference_match_model_tags(language, tag_data_list)


def inference_channel_tags(channel_id):
    logging.info("inference channel tags. channel id: %s" % channel_id)
    channel_data = ChannelData.Load_from_es(channel_id)
    if channel_data is None:
        logging.warn('failed to load channel data. channel id: %s' % channel_id)
        return None
    logging.info('inference channel tags. channel id: %s, language: %s' % (channel_id, channel_data.language))
    inference_tags(channel_data.language, channel_data.tag_data_list)
    return channel_data


def test_channel():
    import sys
    channel_id = sys.argv[1] #'UC8v4vz_n2rys6Yxpj8LuOBA'
    channel_data = inference_channel_tags(channel_id)
    if channel_data:
        for tag_data in channel_data.tag_data_list:
            print(tag_data.get_data())
        #print(channel_data.get_data())
        for k, v in channel_data.get_data().items():
            print(k, ': ', v)


def test():
    test_channel()


if __name__ == '__main__':
    test()







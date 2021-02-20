import os


_cur_dir = os.path.dirname(os.path.realpath(__file__))
_base_dir = os.path.abspath(os.path.join(_cur_dir, '..'))
_data_dir = os.path.join(_base_dir, 'data')
_config_dir = os.path.join(_base_dir, 'config')
_train_data_dir = os.path.join(_data_dir, 'train_data')
_train_model_dir = os.path.join(_data_dir, 'models')
_inference_model_dir = os.path.join(_data_dir, 'models.online')
_unstructure_tag_dir = os.path.join(_config_dir, 'unstructure_tag')

def get_search_video_list_path(language):
    return os.path.join(_train_data_dir, language, 'search_tag_video_id_list.json')

def get_tag_video_dir(language):
    return os.path.join(_train_data_dir, language, 'video_data')

def get_tag_processed_data_dir(language):
    return os.path.join(_train_data_dir, language, 'video_data_processed')

def get_tfidf_tag_feature_data_path(language):
    return os.path.join(_train_data_dir, language, 'tfidf_tag_feature_data.json')

def get_video_data_file_name(level_1_tag, level_2_tag):
    file_name = "%s_%s.json" % (level_1_tag, level_2_tag)
    return file_name

def _get_train_model_dir(language):
    return os.path.join(_train_model_dir, language)

def _get_inference_model_dir(language):
    return os.path.join(_inference_model_dir, language)

def get_train_tfidf_model_dir(language):
    model_dir = _get_train_model_dir(language)
    return os.path.join(model_dir, 'tfidf_model')

def get_inference_tfidf_model_dir(language):
    model_dir = _get_inference_model_dir(language)
    return os.path.join(model_dir, 'tfidf_model')

def get_train_tag_model_dir(language):
    model_dir = _get_train_model_dir(language)
    return os.path.join(model_dir, 'tag_model')

def get_inference_tag_model_dir(language):
    model_dir = _get_inference_model_dir(language)
    return os.path.join(model_dir, 'tag_model')

def get_feature_dict_path():
    return os.path.join(_unstructure_tag_dir, 'feature_word_dict.txt')

def get_stop_words_file_path():
    return os.path.join(_config_dir, 'stop_words.txt')

def get_tag_model_path(language, is_train, model_file_name):
    if is_train:
        model_dir = get_train_tag_model_dir(language)
    else:
        model_dir = get_inference_tag_model_dir(language)
    tag_model_path = os.path.join(model_dir, model_file_name)
    return tag_model_path 



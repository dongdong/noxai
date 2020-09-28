import os

cache_dir = './cache'
train_model_dir = './models'
inference_model_dir = './models.online'

def get_tag_video_list_path(language):
    return os.path.join(cache_dir, language, 'tag_video_id_list.json')

def get_tag_video_data_dir(language):
    return os.path.join(cache_dir, language, 'video_data')

def get_tag_processed_video_data_dir(language):
    return os.path.join(cache_dir, language, 'video_data_processed')

def get_tag_video_feature_data_path(language):
    return os.path.join(cache_dir, language, 'tag_video_feature_data.json')

def get_video_data_file_name(level_1_tag, level_2_tag):
    file_name = "%s_%s.json" % (level_1_tag, level_2_tag)
    return file_name

def _get_train_model_dir(language):
    return os.path.join(train_model_dir, language)

def get_tfidf_train_model_dir(language):
    model_dir = _get_train_model_dir(language)
    return os.path.join(model_dir, 'tfidf_model')

def get_tag_train_model_dir(language):
    model_dir = _get_train_model_dir(language)
    return os.path.join(model_dir, 'tag_model')

def _get_inference_model_dir(language):
    return os.path.join(inference_model_dir, language)

def get_tfidf_inference_model_dir(language):
    model_dir = _get_inference_model_dir(language)
    return os.path.join(model_dir, 'tfidf_model')

def get_tag_inference_model_dir(language):
    model_dir = _get_inference_model_dir(language)
    return os.path.join(model_dir, 'tag_model')





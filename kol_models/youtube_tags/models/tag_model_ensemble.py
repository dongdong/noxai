import logging
from sklearn.model_selection import KFold
from kol_models.youtube_tags.models.tag_model import TagModel
from kol_models.youtube_tags.models.tag_model_nb import TagModel_NB
from kol_models.youtube_tags.models.tag_model_nb import TagModel_NB1
from kol_models.youtube_tags.models.tag_model_nb import TagModel_NB2
from kol_models.youtube_tags.models.tag_model_lm import TagModel_LM
from kol_models.youtube_tags.models.tag_model_lm import TagModel_LM1
from kol_models.youtube_tags.models.tag_model_lm import TagModel_LM2
#from kol_models.youtube_tags.models.tag_model_knn import TagModel_KNN


class PredictTag():
    def __init__(self, model_count):
        self.model_count = model_count
        self.min_count_threshold = self.model_count * 0.6
        self.tag_dict = {}
    def add_tag(self, tag_name, score):
        if tag_name not in self.tag_dict:
            self.tag_dict[tag_name] = []
        self.tag_dict[tag_name].append(score)
    def add_tag_list(self, tag_prob_list):
        print(tag_prob_list)
        for tag_name, prob in tag_prob_list:
            self.add_tag(tag_name, prob)
    def get_tag_list(self):
        tag_list = []
        for tag_name, tag_score_list in self.tag_dict.items():
            tag_size = len(tag_score_list)
            if tag_size < self.min_count_threshold:
                continue
            score = tag_size / self.model_count #* 0.8
            tag_list.append((tag_name, score))
        return tag_list


class TagModel_Ensemble(TagModel):

    def __init__(self):
        self.tag_model_list = []

    def train(self, X, y):
        assert len(self.tag_model_list) > 0
        logging.info('train ensemble tag models. X shape: %s, y shape: %s'
                % (str(X.shape), str(y.shape)))
        model_count = len(self.tag_model_list)
        kf = KFold(n_splits=model_count)
        for tag_model, (train_index, test_index) in zip(
                self.tag_model_list, kf.split(X)):
            X_train, X_eval = X[train_index], X[test_index]
            y_train, y_eval = y[train_index], y[test_index]
            tag_model.train(X_train, y_train)
            tag_model.evaluate(X_eval, y_eval)

    def evaluate(self, X, y):
        assert len(self.tag_model_list) > 0
        logging.info('evaluate ensemble tag models.')
        for tag_model in self.tag_model_list:
            tag_model.evaluate(X, y)

    def predict_batch(self, X):
        assert len(self.tag_model_list) > 0
        return None

    def get_top_prob_tags_batch(self, X, size=3):
        assert len(self.tag_model_list) > 0
        model_count = len(self.tag_model_list)
        item_count, _ = X.shape
        predict_tag_list = [PredictTag(model_count) for i in range(item_count)]
        for tag_model in self.tag_model_list:
            model_prob_list = tag_model.get_top_prob_tags_batch(X, size)
            for predict_tag, tag_prob_list in zip(predict_tag_list, model_prob_list):
                predict_tag.add_tag_list(tag_prob_list)
        top_prob_tags_list = [predict_tag.get_tag_list() 
                for predict_tag in predict_tag_list]
        return top_prob_tags_list

    def load(self):
        logging.info('load ensemble tag models.')
        succ = True
        for tag_model in self.tag_model_list:
            if not tag_model.load():
                succ = False
                break
        return succ

    def save(self):
        logging.info('save ensemble tag models.')
        for tag_model in self.tag_model_list:
            tag_model.save()

    def get_tag_feature_weights_dict(self, size=64):
        tag_feature_dict = {}
        return tag_feature_dict

    @classmethod
    def Get_model(cls, language, tag_level, is_train):
        model = cls()
        tag_model_nb = TagModel_NB.Get_model(language, tag_level, is_train)
        model.tag_model_list.append(tag_model_nb)
        tag_model_nb1 = TagModel_NB1.Get_model(language, tag_level, is_train)
        model.tag_model_list.append(tag_model_nb1)
        #tag_model_nb2 = TagModel_NB2.Get_model(language, tag_level, is_train)
        #model.tag_model_list.append(tag_model_nb2)
        tag_model_lm = TagModel_LM.Get_model(language, tag_level, is_train)
        model.tag_model_list.append(tag_model_lm)
        tag_model_lm1 = TagModel_LM1.Get_model(language, tag_level, is_train)
        model.tag_model_list.append(tag_model_lm1)
        ##tag_model_lm2 = TagModel_LM2.Get_model(language, tag_level, is_train)
        ##model.tag_model_list.append(tag_model_lm2)
        #tag_model_knn = TagModel_KNN.Get_model(language, tag_level, is_train)
        #model.tag_model_list.append(tag_model_knn)
        return model




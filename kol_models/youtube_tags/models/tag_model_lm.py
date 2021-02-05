import logging
from sklearn import linear_model
from sklearn.svm import LinearSVC
from kol_models.youtube_tags.models.tag_model import TagModel


class TagModel_LM(TagModel):

    model_file_name_config_list = ['level_1_tag_sgd.pickle', 'level_2_tag_sgd.pickle']

    def _new_model(self):
        logging.info('create Linear tag model.')
        model = linear_model.SGDClassifier(loss='log')        
        return model


class TagModel_LM1(TagModel):

    model_file_name_config_list = ['level_1_tag_ridge.pickle', 'level_2_tag_ridge.pickle']

    def _new_model(self):
        logging.info('create ridge tag model.')
        model = linear_model.RidgeClassifier()      
        return model

    def predict_batch(self, X):
        assert self.model is not None
        #y_prob_arr = self.model.predict_proba(X)        
        y_prob_arr = self.model.decision_function(X)        
        return y_prob_arr


class TagModel_LM2(TagModel):

    model_file_name_config_list = ['level_1_tag_svc.pickle', 'level_2_tag_svc.pickle']

    def _new_model(self):
        logging.info('create linear svc tag model.')
        model = LinearSVC()      
        return model

    def predict_batch(self, X):
        assert self.model is not None
        #y_prob_arr = self.model.predict_proba(X)        
        y_prob_arr = self.model.decision_function(X)        
        return y_prob_arr


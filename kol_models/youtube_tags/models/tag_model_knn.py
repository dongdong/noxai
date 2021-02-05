import logging
from sklearn.neighbors.nearest_centroid import NearestCentroid
from kol_models.youtube_tags.models.tag_model import TagModel


class TagModel_KNN(TagModel):

    model_file_name_config_list = ['level_1_tag_knn.pickle', 'level_2_tag_knn.pickle']

    def _new_model(self):
        logging.info('create nearest centroid tag model.')
        model = NearestCentroid()
        return model

    def predict_batch(self, X):
        assert self.model is not None
        y_arr = self.model.predict(X)        
        return y_arr

    def get_top_prob_tags_batch(self, X, size=3):
        y_arr = self.predict_batch(X)
        top_list = [[(tag, 1.0)] for tag in y_arr]
        return top_list



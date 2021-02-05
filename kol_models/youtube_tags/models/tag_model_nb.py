import logging
from sklearn.naive_bayes import MultinomialNB, ComplementNB, BernoulliNB
from kol_models.youtube_tags.models.tag_model import TagModel


class TagModel_NB(TagModel):

    model_file_name_config_list = ['level_1_tag_nb.pickle', 'level_2_tag_nb.pickle']

    def _new_model(self):
        logging.info('create NB tag model.')
        model = ComplementNB() 
        #model = MultinomialNB() 
        return model


class TagModel_NB1(TagModel):

    model_file_name_config_list = [
        'level_1_tag_multinomial_nb.pickle', 
        'level_2_tag_multinomial_nb.pickle',
    ]

    def _new_model(self):
        logging.info('create NB tag model.')
        #model = ComplementNB() 
        model = MultinomialNB() 
        return model


class TagModel_NB2(TagModel):

    model_file_name_config_list = [
        'level_1_tag_bernoulli_nb.pickle', 
        'level_2_tag_bernoulli_nb.pickle',
    ]

    def _new_model(self):
        logging.info('create NB tag model.')
        #model = ComplementNB() 
        #model = MultinomialNB() 
        model = BernoulliNB()
        return model


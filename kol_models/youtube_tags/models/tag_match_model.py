from collections import defaultdict

_match_words_tag_config = {
    'Unboxing': ['unboxing', '开箱', '開箱', '언박싱'], 
    'VLOG': ['vlog', 'vloger', 'vlogging', 'vlogs', '브이로그'], 
    'ASMR': ['asmr'],
    'MUKBANG': ['mukbang', '먹방'],
    'DIY': ['diy', 'do it yourself', 'doityourself']
}

_count_score_arr = [0.5, 0.7, 0.8]

def get_all_tags():
    return _match_words_tag_config.keys()


class TagMatchModel():
    def __init__(self, match_words_tag_config, count_score_arr):
        self.match_word_tag_map = {}
        for tag_class, match_word_list in match_words_tag_config.items():
            for match_word in match_word_list:
                if match_word not in self.match_word_tag_map:
                    self.match_word_tag_map[match_word] = []
                self.match_word_tag_map[match_word].append(tag_class)
        #print(self.match_word_tag_map)
        for match_word, tag_class_list in self.match_word_tag_map.items():
            self.match_word_tag_map[match_word] = list(set(tag_class_list))
        self.count_score_arr = count_score_arr

    def get_tag_list(self, feature_words):
        tag_counter = defaultdict(int)
        for feature_word in feature_words:
            if feature_word in self.match_word_tag_map:
                for tag_class in self.match_word_tag_map[feature_word]:
                    tag_counter[tag_class] += 1
        #print(tag_counter.items())
        tag_list = []
        total_count = sum(tag_counter.values())
        tag_count = len(tag_counter.keys())
        total_score = 0
        for tag_class, count in tag_counter.items():
            if count > len(self.count_score_arr):
                score_index = len(self.count_score_arr) - 1
            else:
                score_index = count - 1            
            tag_score = self.count_score_arr[score_index] #* count / total_count 
            tag_list.append((tag_class, tag_score))
            total_score += tag_score
        #normalize and penalty too many tags
        if len(tag_list) > 2:
            #tag_list = [(tag_class, (tag_score / total_score) * (2 / tag_count)) 
            #for tag_class, tag_score in tag_list:
            #    print(tag_class, tag_score, tag_count, tag_score * (2 / tag_count))
            tag_list = [(tag_class, tag_score * (2 / tag_count)) 
            #tag_list = [(tag_class, tag_score / (tag_count - 1)) 
                    for tag_class, tag_score in tag_list]
        return tag_list

    def get_tag_list_batch(self, feature_words_list):
        tag_list_all = []
        for feature_words in feature_words_list:
            tag_list = self.get_tag_list(feature_words)
            tag_list_all.append(tag_list)
        return tag_list_all


_tag_match_model = TagMatchModel(_match_words_tag_config, _count_score_arr)
def inference_tags(tag_data_list):
    feature_words_list = [tag_data.feature_words 
            for tag_data in tag_data_list]
    tag_list_all = _tag_match_model.get_tag_list_batch(feature_words_list)
    tag_dict = {}
    for tag_data, tag_list in zip(tag_data_list, tag_list_all):
        tag_data.tag_match_model_tags = tag_list
        tag_dict[tag_data.item_id] = tag_list
    return tag_dict


def test():
    feature_words_list = [
        ['diy', 'do it yourself', 'vlog'],
        ['diy', 'do it yourself', 'vlog', 'asmr'],
        ['asmr', 'mukbang'],    
        ['vlog', 'vloger', 'vlogging'],    
        ['diy', 'vlog', 'asmr', 'unboxing'],    
        ['diy', 'vlog', 'vloger', 'vlogging', 'asmr', 'unboxing'],    
        ['tag', 'model', 'list'],    
        ['tag', 'model', 'list', 'doityourself'],    
    ]    
    tag_list_all = _tag_match_model.get_tag_list_batch(feature_words_list)
    for feature_words, tag_list in zip(feature_words_list, tag_list_all):
        print('features: ', feature_words)
        print('tags: ', tag_list)


if __name__ == '__main__':
    test()



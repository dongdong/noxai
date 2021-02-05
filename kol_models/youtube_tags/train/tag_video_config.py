import json

import kol_models.youtube_tags.commons.path_manager as pm

def _get_tag_video_id_dict(search_video_id_path): 
    tag_video_id_dict = {}
    with open(search_video_id_path, 'r') as f:
        tag_video_dict = json.load(f)
        for level_1_tag, level_2_dict in tag_video_dict.items():
            tag_video_id_dict[level_1_tag] = {}
            for level_2_tag, keyword_dict in level_2_dict.items():
                video_id_list = []
                for keyword, keyword_id_list in keyword_dict.items():
                    video_id_list += keyword_id_list
                video_id_list = list(set(video_id_list))
                tag_video_id_dict[level_1_tag][level_2_tag] = video_id_list
    return tag_video_id_dict
 

class TagVideoConfig(object):
    def __init__(self, tag_video_id_dict):
        self.tag_video_id_dict = tag_video_id_dict

    def get_level_1_tags(self):
        return self.tag_video_id_dict.keys()

    def get_level_2_tags(self, level_1_tag):
        return self.tag_video_id_dict[level_1_tag].keys()

    def iter_tags(self):
        for level_1_tag, level_2_dict in self.tag_video_id_dict.items():
            for level_2_tag, _ in level_2_dict.items(): 
                yield (level_1_tag, level_2_tag)

    def get_video_id_list(self, level_1_tag, level_2_tag):
        video_id_list = []
        if (level_1_tag in self.tag_video_id_dict 
                and level_2_tag in self.tag_video_id_dict[level_1_tag]):
            video_id_list = self.tag_video_id_dict[level_1_tag][level_2_tag]
        return video_id_list

    @staticmethod
    def Load(language):
        search_video_id_path = pm.get_search_video_list_path(language)
        tag_video_id_dict = _get_tag_video_id_dict(search_video_id_path)
        # @TODO add ther tagged data to dict
        tag_video_config = TagVideoConfig(tag_video_id_dict)
        return tag_video_config

    
def test(language):
    tag_video_config = TagVideoConfig.Load(language)
    for level_1_tag, level_2_tag in tag_video_config.iter_tags():
        print(level_1_tag, level_2_tag)
    video_id_list = tag_video_config.get_video_id_list(level_1_tag, level_2_tag)
    print(video_id_list)


if __name__ == '__main__':
    test('zh')

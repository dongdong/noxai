import os
import json
import logging

from tag_config import tag_name_config_list 
from tag_config import tag_search_keywords_config
from tag_config import youtube_topic_id_en_name_map 
from mysql_utils import get_tag_info_rows


def get_tag_level_map(langauge):
    tag_level_1_2_map = {}
    tag_level_2_1_map = {}
    tag_level_info_list = tag_search_keywords_config[langauge]
    for tag_level_info in tag_level_info_list:
        level_1_tag = tag_level_info['level_1']
        level_2_tag = tag_level_info['level_2']
        if level_1_tag not in tag_level_1_2_map:
            tag_level_1_2_map[level_1_tag] = []
        tag_level_1_2_map[level_1_tag].append(level_2_tag)
        tag_level_2_1_map[level_2_tag] = level_1_tag
    return tag_level_1_2_map, tag_level_2_1_map


def get_topic_tag_name_map(language):
    tag_name_map = {}
    topic_tag_name_map = {}
    for item in tag_name_config_list:
        if 'en' not in item or language not in item:
            continue
        en_name = item['en']
        name = item[language]
        if en_name != '' and name != '':
            tag_name_map[en_name] = name
    for topic_id, en_name in youtube_topic_id_en_name_map.items(): 
        if en_name in tag_name_map:
            topic_tag_name_map[topic_id] = tag_name_map[en_name]
        else:
            logging.warning('Unknown topic config. topic id: %s, en name: %s' % (topic_id, en_name))
            topic_tag_name_map[topic_id] = en_name
    return topic_tag_name_map


def get_tag_name_id_map(language):
    tag_name_id_map = {}
    for item in tag_name_config_list:
        if 'id' not in item or language not in item:
            continue
        tag_id = item['id']
        tag_name = item[language]
        if tag_name != '':
            tag_name_id_map[tag_name] = tag_id
    return tag_name_id_map
  
  
def get_structure_tag_info():
    rows = get_tag_info_rows()
    structure_tag_info_map = {}
    for row in rows:
        tag_id, parent_id, en_name, zh_name, _, ko_name, _ = row
        if parent_id is None:
            level = 1
        else:
            level = 2
        structure_tag_info_map[tag_id] = {
            'id': tag_id,
            'level': level,
            'en': en_name,
            'zh': zh_name, 
            'ko': ko_name,
        }
    return structure_tag_info_map


class ModelTagConfig():
    def __init__(self, tag_level_1_2_map, tag_level_2_1_map):
        self.tag_level_1_2_map = tag_level_1_2_map
        self.tag_level_2_1_map = tag_level_2_1_map

    @staticmethod
    def Create(language):
        tag_level_1_2_map, tag_level_2_1_map = get_tag_level_map(language)
        logging.info("create model tag config for language: %s. level 1 tag size: %d, level 2 tag size: %d" % 
                (language, len(tag_level_1_2_map.keys()), len(tag_level_2_1_map.keys())))
        model_tag_config = ModelTagConfig(tag_level_1_2_map, tag_level_2_1_map)
        return model_tag_config

    def get_parent_tag_name(self, tag_name):
        parent_name = None
        if tag_name in self.tag_level_2_1_map:
            parent_name = self.tag_level_2_1_map[tag_name]
        return parent_name

    def is_level_1_tag_name(self, tag_name):
        return tag_name in self.tag_level_1_2_index_map

    def get_tag_iter(self):
        for level_1_tag, level_2_tag_list in self.tag_level_1_2_map.items():
            for level_2_tag in level_2_tag_list:
                yield level_1_tag, level_2_tag
 

class TagIndex():
    def __init__(self, topic_tag_name_map, tag_name_id_map, structure_tag_info_map):
        self.topic_tag_name_map = topic_tag_name_map
        self.tag_name_id_map = tag_name_id_map
        self.structure_tag_info_map = structure_tag_info_map
        self.topic_id_black_set = set(['/m/019_rr', ])
        self.unknown_topic_id_set = set()

    @staticmethod
    def Create(language):
        topic_tag_name_map = get_topic_tag_name_map(language)
        tag_name_id_map = get_tag_name_id_map(language)
        structure_tag_info_map = get_structure_tag_info()

        tag_index = TagIndex(topic_tag_name_map, tag_name_id_map, structure_tag_info_map) 
        return tag_index
   
    def get_tag_name_by_topic_id(self, topic_id):
        tag_name = None
        if topic_id in self.topic_tag_name_map:
            tag_name = self.topic_tag_name_map[topic_id]
        elif topic_id not in self.topic_id_black_set:
            logging.warn('Unknown topic, topid id: %s' % topic_id)
            self.unknown_topic_id_set.add(topic_id)
        else:
            pass
        return tag_name

    def get_structure_tag_info(self, tag_name):
        structure_tag_info = None
        if tag_name in self.tag_name_id_map:
            tag_id = self.tag_name_id_map[tag_name]
            if tag_id in self.structure_tag_info_map:
                structure_tag_info = self.structure_tag_info_map[tag_id] 
        return structure_tag_info 



def test(language):
    tag_level_1_2_map, tag_level_2_1_map = get_tag_level_map(language)
    print(tag_level_1_2_map)    
    print(tag_level_2_1_map)    

    topic_tag_name_map = get_topic_tag_name_map(language)
    print(topic_tag_name_map)

    tag_name_id_map = get_tag_name_id_map(language)
    print(tag_name_id_map)

    structure_tag_info_map = get_structure_tag_info()
    print(structure_tag_info_map)

    for tag_name, tag_id in tag_name_id_map.items():
        if tag_id in structure_tag_info_map:
            structure_tag_info = structure_tag_info_map[tag_id]
        else:
            structure_tag_info = None
        print(tag_name, structure_tag_info)


if __name__ == '__main__':
    language = 'zh-Hans'
    #language = 'en'
    test(language)



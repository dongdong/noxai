import logging
from kol_models.commons.youtube_api import get_youtube_video_topics_batch
from kol_models.commons.youtube_api import get_youtube_channel_topics_batch
from kol_models.youtube_tags.commons.tag_data import ItemType

_tag_score = 0.8

_topic_id_tag_map = {
    '/m/04rlf': 'Music',
    '/m/05fw6t': "Children's music",
    '/m/02mscn': 'Christian music',
    '/m/0ggq0m': 'Classical music',
    '/m/01lyv': 'Country music',
    '/m/02lkt': 'Electronic music',
    '/m/0glt670': 'Hip Hop',
    '/m/05rwpb': 'Independent music',
    '/m/03_d0': 'Jazz',
    '/m/028sqc': 'Music of Asia',
    '/m/0g293': 'Music of Latin America',
    '/m/064t9': 'Pop music',
    '/m/06cqb': 'Reggae',
    '/m/06j6l': 'Rhythm & Blues',
    '/m/06by7': 'Rock music',
    '/m/0gywn': 'Soul music',
    '/m/0bzvm2': 'Gaming',
    '/m/025zzc': 'Action game',
    '/m/02ntfj': 'Action-adventure game',
    '/m/0b1vjn': 'Casual game',
    '/m/02hygl': 'Music game',
    '/m/04q1x3q': 'Puzzle game',
    '/m/01sjng': 'Racing game',
    '/m/0403l3g': 'Role-playing game',
    '/m/021bp2': 'Simulation game',
    '/m/022dc6': 'Sports game',
    '/m/03hf_rm': 'Strategy game',
    '/m/06ntj': 'Sports',
    '/m/0jm_': 'American football',
    '/m/018jz': 'Baseball',
    '/m/018w8': 'Basketball',
    '/m/01cgz': 'Boxing',
    '/m/09xp_': 'Cricket',
    '/m/02vx4': 'Football',
    '/m/037hz': 'Golf',
    '/m/03tmr': 'Ice hockey',
    '/m/01h7lh': 'Mixed martial arts',
    '/m/0410tth': 'Motorsport',
    '/m/066wd': 'Professional wrestling',
    '/m/07bs0': 'Tennis',
    '/m/07_53': 'Volleyball',
    '/m/02jjt': 'Entertainment',
    '/m/095bb': 'Animated cartoon',
    #'/m/095bb': 'Cartoon & Animation',
    '/m/09kqc': 'Humor',
    '/m/02vxn': 'Movie',
    '/m/0f2f9': 'TV shows',
    '/m/05qjc': 'Performing arts',
    '/m/019_rr': 'Life Style',
    '/m/032tl': 'Fashion',
    '/m/027x7n': 'Fitness',
    '/m/02wbm': 'Food',
    '/m/0kt51': 'Health',
    '/m/03glg': 'Hobby',
    '/m/068hy': 'Pets',
    '/m/041xxh': 'Beauty',
    '/m/07c1v': 'Technology',
    #'/m/07c1v': 'Science & Technology',
    #'/m/07bxq': 'Tourism',
    '/g/120yrv6h': 'Tourism',
    #'/g/120yrv6h': 'Travel',
    '/m/07yv9': 'Vehicles',
    '/m/01k8wb': 'Knowledge',
    '/m/098wr': 'Society',
    '/m/01h6rj': 'Military',
    '/m/05qt0': 'Politics',            
    '/m/06bvp': 'Religion',
}

_topic_category_tag_map = {
    'Entertainment': 'Entertainment',
    'Television_program':  'TV shows', #
    'Film': 'Movie', #
    'Humour': 'Humour',
    'Performing_arts': 'Performing arts',
    #'Animated cartoon': 'Animated cartoon',

    'Music': 'Music',
    'Electronic_music': 'Electronic music',
    'Hip_hop_music': 'Hip hop', #
    'Pop_music': 'Pop music',
    'Soul_music': 'Soul music',
    'Music_of_Latin_America': 'Music of Latin America',
    'Music_of_Asia': 'Music of Asia',
    'Rhythm_and_blues': 'Rhythm & Blues',
    'Rock_music': 'Rock music',
    'Independent_music': 'Independent music',
    'Christian_music': 'Christian music',
    'Country_music': 'Country music',
    'Classical_music': 'Classical music',
    'Reggae': 'Reggae',
    'Jazz': 'Jazz',
    #"Children's music": "Children's music",

    'Video_game_culture': 'Gaming',
    'Action-adventure_game': 'Action-adventure game',
    'Action_game': 'Action game',
    'Role-playing_video_game': 'Role-playing game',
    'Strategy_video_game': 'Strategy game',
    'Simulation_video_game': 'Simulation game',
    'Casual_game': 'Casual game',
    'Sports_game': 'Sports game',
    'Racing_video_game': 'Racing game',
    'Music_video_game': 'Music game',
    'Puzzle_video_game': 'Puzzle game',

    'Sport': 'Sports',
    'Association_football': 'Football',
    'Motorsport': 'Motorsport',
    'Mixed_martial_arts': 'Mixed martial arts',
    'Boxing': 'Boxing',
    'Tennis': 'Tennis',
    'Basketball': 'Basketball',
    'Professional_wrestling': 'Professional wrestling',
    'Volleyball': 'Volleyball',
    'Baseball': 'Baseball',
    'American_football': 'American football',
    'Golf': 'Golf',
    'Cricket': 'Cricket',
    'Ice_hockey': 'Ice hockey',

    'Hobby': 'Hobby',
    'Tourism': 'Tourism',
    'Vehicle': 'Vehicle',
    'Food': 'Food',
    'Pet': 'Pet',
    'Health': 'Health',
    'Fashion': 'Fashion',
    'Physical_fitness': 'Fitness',

    'Technology': 'Technology',
    'Knowledge': 'Knowledge',
    'Society': 'Society',
    'Military': 'Military',
    'Religion': 'Religion',
    'Politics': 'Politics',
    'Business': 'Business',
    
    #'Physical_attractiveness': 'Physical attractiveness', #?
    'Physical_attractiveness': 'Beauty',
    'Lifestyle_(sociology)': 'Lifestyle',
}


def get_all_tag_names():
    return _id_tag_map.values() 


def inference_tags(tag_data_list):
    video_id_list = [tag_data.item_id for tag_data in tag_data_list
            if tag_data.item_type == ItemType.VIDEO]
    ytb_topic_dict = get_youtube_video_topics_batch(video_id_list)
    channel_id_list = [tag_data.item_id for tag_data in tag_data_list
            if tag_data.item_type == ItemType.CHANNEL]
    ytb_channel_topic_dict = get_youtube_channel_topics_batch(channel_id_list)
    ytb_topic_dict.update(ytb_channel_topic_dict)
    ytb_tag_dict = {}
    for tag_data in tag_data_list:
        if tag_data.item_id in ytb_topic_dict:
            ytb_topic_list = ytb_topic_dict[tag_data.item_id]
            tag_list = []
            for ytb_topic in ytb_topic_list:
                if ytb_topic in _topic_category_tag_map:
                    tag_name = _topic_category_tag_map[ytb_topic]
                    tag_list.append(tag_name)
                else:
                    logging.warn('Unknown youtube topic: %s' % ytb_topic)
            tag_data.ytb_topic_tags = tag_list
            ytb_tag_dict[tag_data.item_id] = tag_list
    return ytb_tag_dict


def test():
    from kol_models.commons.es_utils import get_video_contents
    from kol_models.youtube_tags.commons.video_data import VideoData
    from kol_models.youtube_tags.commons.channel_data import ChannelData
    from kol_models.youtube_tags.commons.tag_data import PredictTagData

    video_id_list = [
        '9K_CZizKdVs',
        'StODEPNXXdQ',
        'E0IZaVVGZfU',
        'E_kEZpD_ZN0',
        'PcN4A9fixGA',
        'oWkOkpzyD3Y',
        'A-YcBYUfgtc',
        'djHIlYNvT8M',
    ]
    channel_id_list = [
        'UCRXiA3h1no_PFkb1JCP0yMA',
        'UC8v4vz_n2rys6Yxpj8LuOBA',
        'UC8gI5aiUi-9CyG4TGH4eugQ',
        'UCGRODP7wtTU_uyNZmu9cb7g',
        'UCu9MYfF0vosVcK38oNnnJxw',
        'UCLsooMJoIpl_7ux2jvdPB-Q',
        'UCJjSDX-jUChzOEyok9XYRJQ',
        'UCmvAV_za1KXGXSgC0o0v9DQ',
    ]
    tag_data_list = []
    for video_id in video_id_list:
        #video_data = VideoData.Load_from_es(video_id)
        #if video_data:
        tag_data = PredictTagData.From_video(video_id, None)
        tag_data_list.append(tag_data)
    for channel_id in channel_id_list:
        #channel_data = ChannelData.Load_from_es(channel_id)
        #if channel_data:
        tag_data = PredictTagData.From_channel(channel_id, None)
        tag_data_list.append(tag_data) 
    inference_tags(tag_data_list)
    for tag_data in tag_data_list:
        print(tag_data.get_data())

if __name__ == '__main__':
    test()

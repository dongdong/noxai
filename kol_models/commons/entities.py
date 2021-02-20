
class DictWrapper(object):
    def __init__(self, data_dict): 
        super(DictWrapper, self).__init__()
        self._data_dict = data_dict

    def __getattr__(self, name):
        #print('DictWrapper.__getattr__(%s)' % name)
        value = None
        if name in self._data_dict:
            value = self._data_dict[name]
            setattr(self, name, value)
        return value


class YoutubeChannel(DictWrapper):
    def __init__(self, channel_data_dict):
        super().__init__(channel_data_dict)

    def get_data(self):
        data = {
            'channel_id': self.channel_id,
            'title': self.title,
            'introduction': self.introduction, 
            'keywords': self.keywords,
            'country_code': self.country_code,
            'languages': self.languages,
            'cateogry_from_mysql': self.category_from_mysql, 
            'category': self.category,
            'sub_num': self.sub_num, 
            'total_videos': self.total_videos,
            'latest_three_pub_date': self.latest_three_pub_date,
            'related_channel_list': self.related_channel_list,
            'status': self.status,
            'is_delete': self.is_delete, 
        }
        return data


class YoutubeVideo(DictWrapper):
    def __init__(self, video_data_dict): 
        super().__init__(video_data_dict)
    
    #def __getattr__(self, name):
    #    print('YoutubeVideo.__getattr__(%s)' % name)
    #    return super().__getattr__(name)

    def get_data(self):
        data = {
            'video_id': self.video_id,
            'title': self.title,
            'category_id': self.category_id,
            'keywords': self.keywords,
            'source': self.source,
        }
        return data


def test_video():
    def dump_video_contents(video_contents):
        video = YoutubeVideo(video_contents)
        print(video.get_data())
        print(video.get_data())
    from es_utils import get_video_contents as get_video_contents_from_es
    from youtube_api import get_video_contents as get_video_contents_from_ytb
    video_id = 'YWn9xIGbZfw'
    video_contents_es = get_video_contents_from_es(video_id)
    dump_video_contents(video_contents_es)
    video_contents_ytb = get_video_contents_from_ytb(video_id)
    dump_video_contents(video_contents_ytb)


def test_channel():
    from es_utils import get_channel_contents
    channel_id = 'UCRs1pHnES3QDdh43xbjOmzw'
    channel_contents = get_channel_contents(channel_id)
    channel = YoutubeChannel(channel_contents)
    print(channel.get_data())
    print(channel.get_data())

if __name__ == '__main__':
    test_video()
    test_channel()



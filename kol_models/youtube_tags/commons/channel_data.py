import datetime
import logging
from collections import Counter

from kol_models.commons.entities import YoutubeChannel
from kol_models.commons.es_utils import get_channel_contents, get_channel_video_list
from kol_models.youtube_tags.commons.video_data import VideoData, parse_video_text, clean_keyword
from kol_models.youtube_tags.commons.tag_data import PredictTagData

CHANNEL_VIDEO_LIST_SIZE = 32
VIDEO_PUB_DAYS_THRESHOLD = 365 * 3
CHANNEL_VIDEO_COUNT_MIN = 8
CHANNEL_FEATURE_WORD_COUNT_MIN = 3
CHANNEL_META_VIDEO_DATA_SIZE = 3

_now = datetime.datetime.now()
_pub_datatime_threshold = _now - datetime.timedelta(days=VIDEO_PUB_DAYS_THRESHOLD)
_pub_time_threshold = _pub_datatime_threshold.timestamp()
_pub_date_threshold = _pub_datatime_threshold.strftime("%Y-%m-%d")


def _get_channel_video_list(channel_contents, size):
    video_data_list = []
    channel_id = channel_contents['channel_id']
    language = channel_contents.get('languages', '')
    logging.info('get channel video list. channel id: %s, language: %s' 
            % (channel_id, language))
    total_videos = channel_contents.get('total_videos', 0)
    if total_videos < CHANNEL_VIDEO_COUNT_MIN:
        logging.warn('channel do not have enough videos. video count: %d' % total_videos)
        return video_data_list
    video_contents_list = get_channel_video_list(channel_id, size, _pub_date_threshold)
    if len(video_contents_list) < CHANNEL_VIDEO_COUNT_MIN:
        logging.warn('channel do not have enough valid videos. video count: %d, valid: %d' 
                % (total_videos, len(video_contents_list)))
        return video_data_list
    for video_contents in video_contents_list:
        if video_contents:
            video_data = VideoData(video_contents)
            parse_video_text(video_data, language)
            video_data_list.append(video_data)
    return video_data_list


def _is_valid_channel_contents(channel_id, channel_contents):
    if not channel_contents:
        logging.info('invalid channel contents: empty contents. channel id: %s' 
                % channel_id)
        return False
    is_delete = channel_contents.get('is_delete', 0)
    if is_delete != 1:
        logging.info('invalid channel contents: channel has been deleted. channel id: %s' 
                % channel_id)
        return False
    return True


class ChannelData(YoutubeChannel):
    def __init__(self, channel_data_dict):
        super().__init__(channel_data_dict)
        self.video_data_list = None
        self._clean_keywords = None
        self._feature_words = None
        self._tag_data_list = None

    @property
    def language(self):
        return self.languages

    def _get_clean_keywords(self):
        clean_keywords = []
        if self.keywords:
            for keyword in self.keywords:
                clean_word = clean_keyword(keyword)
                if clean_word:
                    clean_keywords.append(clean_word)
        return clean_keywords

    @property
    def clean_keywords(self):
        if self._clean_keywords is None:
            self._clean_keywords = self._get_clean_keywords()
        return self._clean_keywords

    def _get_feature_words(self):
        word_counter = Counter()
        word_counter.update(self.clean_keywords)
        for video_data in self.video_data_list:
            #word_counter.update(video_data.feature_words)
            word_counter.update(video_data.clean_keywords)
        feature_words = [word for word, count in word_counter.items() 
                if count > CHANNEL_FEATURE_WORD_COUNT_MIN] 
        return feature_words

    @property
    def feature_words(self):
        if self._feature_words is None:
            self._feature_words = self._get_feature_words()
        return self._feature_words

    def _get_tag_data_list(self):
        _tag_data_list = [PredictTagData.From_video_data(video_data) 
                for video_data in self.video_data_list]
        channel_tag_data = PredictTagData.From_channel(self.channel_id, self.feature_words)
        _tag_data_list.append(channel_tag_data)
        return _tag_data_list

    @property
    def tag_data_list(self):
        if self._tag_data_list is None:
            self._tag_data_list = self._get_tag_data_list()
        return self._tag_data_list

    def get_data(self):
        data = super().get_data()
        if self._clean_keywords:
            data['clean_keywrods'] = self._clean_keywords
        if self._feature_words:
            data['feature_words'] = self._feature_words
        if self._tag_data_list:
            data['tag_data_list'] = self._tag_data_list
        return data

    @staticmethod
    def Load_from_es(channel_id, video_list_size=CHANNEL_VIDEO_LIST_SIZE):
        channel_data = None
        try:
            channel_contents = get_channel_contents(channel_id) 
            if _is_valid_channel_contents(channel_id, channel_contents):
                channel_data = ChannelData(channel_contents)
                channel_data.video_data_list = _get_channel_video_list(channel_contents, 
                        video_list_size)
        except:
            logging.warn('failed to load channel data! channel id: %s' % channel_id)
        return channel_data


def test():
    channel_id = 'UCuW8NuYXAG2sAP0HldHgNiw'
    channel_data = ChannelData.Load_from_es(channel_id)
    for k, v in channel_data.get_data().items():
        print(k, v)


if __name__ == '__main__':
    test()

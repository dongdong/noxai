import redis
import os
import time
from datetime import datetime
import json
import fcntl

import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')

pool_spider = redis.ConnectionPool(
        host='10.100.1.254', 
        password='dhmi5xMPjjfidUx4', 
        port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)

cur_dir = os.path.dirname(os.path.realpath(__file__))
default_log_dir = os.path.join(cur_dir, 'pipe_record_logs')

class PipeRecordLogLock(object):
    def __init__(self, lock_name, lock_path):
        self.lock_name = lock_name
        self.lock_file = open(lock_path, 'w')

    def __del__(self):
        if self.lock_file:
            self.lock_file.close()
            #print('close lock file')

    def lock(self):
        logging.info('try lock %s' % self.lock_name)
        fcntl.flock(self.lock_file, fcntl.LOCK_EX)
        logging.info('lock %s' % self.lock_name)
        time.sleep(3)

    def unlock(self):
        fcntl.flock(self.lock_file, fcntl.LOCK_UN)
        logging.info('unlock %s' % self.lock_name)


class DataOpMixin(object):
    def _load_data(self, path):
        data = None
        if os.path.exists(path):
            with open(path, 'r') as f:
                data = json.load(f)
        return data

    def _dump_data(self, path, data):
        with open(path, 'w') as f:
            json.dump(data, f)

    def _dump_list(self, path, item_list):
        with open(path, 'a') as f:
            for item in item_list:
                f.write(item + '\n')


class PipeRecordLog(DataOpMixin):   
    def __init__(self, pipe_name, log_dir):
        self.pipe_name = pipe_name
        self.log_dir = log_dir
        pipe_log_file_name = "%s.json" % pipe_name
        self.pipe_log_path = os.path.join(self.log_dir, pipe_log_file_name)
        pipe_info = self._load_data(self.pipe_log_path)
        self._set_cur_project_info(pipe_info)

    def _set_cur_project_info(self, pipe_info):
        if pipe_info:
            self.cur_project_name = pipe_info['current_project_name']
            self.cur_project_path = pipe_info['current_project_path']
            lock_file_name = '%s.lock' % self.cur_project_name
            lock_path = os.path.join(self.log_dir, lock_file_name)
            self.cur_project_lock = PipeRecordLogLock(lock_file_name, lock_path)
        else:
            self.cur_project_name = None
            self.cur_project_path = None
            self.cur_project_lock = None

    def create_project(self):
        logging.info('PipeRecordLog.create_project')
        datetime_now = datetime.now()
        str_now = datetime.strftime(datetime_now, '%Y%m%d%H%M%S')
        project_name = '%s_%s' % (self.pipe_name, str_now)
        project_file_name = '%s.json' % project_name
        project_path = os.path.join(self.log_dir, project_file_name)
        project_log_info = {
            'project_name': project_name,
            'total_produced_id_count': 0,
            'total_consumed_id_count': 0,
            'producers': {},
            'consumers': {}
        }
        self._dump_data(project_path, project_log_info)
        pipe_info = {
            'current_project_name': project_name,
            'current_project_path': project_path,
        }
        self._dump_data(self.pipe_log_path, pipe_info)
        self._set_cur_project_info(pipe_info)
            
    def add_producer(self):
        logging.info('PipeRecordLog.add_producer')
        assert self.cur_project_lock is not None
        self.cur_project_lock.lock()
        try:
            project_log_info = self._load_data(self.cur_project_path)
            assert project_log_info is not None
            datetime_now = datetime.now()
            str_now = datetime.strftime(datetime_now, '%Y%m%d%H%M%S')
            producer_name = '%s_producer_%s' % (project_log_info['project_name'], str_now)
            if producer_name not in project_log_info['producers']:
                producer_log_name = '%s.log' % producer_name
                producer_log_path = os.path.join(self.log_dir, producer_log_name)
                project_log_info['producers'][producer_name] = {
                    'producer_log_path': producer_log_path,
                    'produced_id_count': 0,
                }
                self._dump_data(self.cur_project_path, project_log_info)
            else:
                # producer already exists, do nothing
                raise Exception('producer already exists!') 
        finally:
            if self.cur_project_lock:
                self.cur_project_lock.unlock()
        return producer_name, producer_log_path
    
    def update_producer(self, producer_name, id_count):
        logging.info('PipeRecordLog.update_producer %s %s' % (producer_name, id_count))
        assert self.cur_project_lock is not None
        self.cur_project_lock.lock()
        try:
            project_log_info = self._load_data(self.cur_project_path)
            assert project_log_info is not None
            assert producer_name in project_log_info['producers']
            project_log_info['producers'][producer_name] = {'produced_id_count': id_count}
            project_log_info['total_produced_id_count'] += id_count
            self._dump_data(self.cur_project_path, project_log_info)
        finally:
            if self.cur_project_lock:
                self.cur_project_lock.unlock()

    def add_consumer(self):
        logging.info('PipeRecordLog.add_consumer')
        assert self.cur_project_lock is not None
        self.cur_project_lock.lock()
        try:
            project_log_info = self._load_data(self.cur_project_path)
            assert project_log_info is not None
            datetime_now = datetime.now()
            str_now = datetime.strftime(datetime_now, '%Y%m%d%H%M%S')
            consumer_name = '%s_consumer_%s' % (project_log_info['project_name'], str_now)
            consumer_log_name = '%s.log' % consumer_name
            consumer_log_path = os.path.join(self.log_dir, consumer_log_name)
            project_log_info['consumers'][consumer_name] = {
                'consumer_log_path': consumer_log_path,
                'consumed_id_count': 0,
            }
            self._dump_data(self.cur_project_path, project_log_info)
        finally:
            if self.cur_project_lock:
                self.cur_project_lock.unlock()
        return consumer_name, consumer_log_path
 
    def update_consumer(self, consumer_name, id_count):
        logging.info('PipeRecordLog.update_consumer %s %s' % (consumer_name, id_count))
        assert self.cur_project_lock is not None
        self.cur_project_lock.lock()
        try:
            project_log_info = self._load_data(self.cur_project_path)
            assert project_log_info is not None
            assert consumer_name in project_log_info['consumers']
            project_log_info['consumers'][consumer_name] = {'consumed_id_count': id_count}
            project_log_info['total_consumed_id_count'] += id_count
            self._dump_data(self.cur_project_path, project_log_info)
        finally:
            if self.cur_project_lock:
                self.cur_project_lock.unlock()

    def dump_ids(self, log_path, id_list):
        self._dump_list(log_path, id_list)


class ChannelIdPipe(object):
    def __init__(self, pipe_name, record_log_dir=None):
        super(ChannelIdPipe, self).__init__()
        self.pipe_name = pipe_name
        if record_log_dir is None:
            record_log_dir = default_log_dir
        self.pipe_record_log = PipeRecordLog(pipe_name, record_log_dir)
   
    def get_length(self):
        return redis_spider.llen(self.pipe_name)

    def _insert_channel_ids(self, channel_id_iter):
        total_num = 0
        batch_size = 1000
        id_record_list = []
        producer_name, producer_log_path = self.pipe_record_log.add_producer()
        try:
            for channel_id in channel_id_iter:
                redis_spider.rpush(self.pipe_name, channel_id)
                total_num += 1
                id_record_list.append(channel_id)
                if total_num % batch_size == 0:
                    logging.info('writing channel id to pipe. job name: %s, total: %d, currnet length: %d' 
                            % (producer_name, total_num, self.get_length()))
                    self.pipe_record_log.dump_ids(producer_log_path, id_record_list)
                    id_record_list.clear()
            logging.info('write channel id to redis finish! total: %d' % (total_num))
        finally:
            if id_record_list:
                self.pipe_record_log.dump_ids(producer_log_path, id_record_list)
            if total_num > 0:
                self.pipe_record_log.update_producer(producer_name, total_num)

    def clean(self):
        redis_spider.delete(self.pipe_name)
        succ = not redis_spider.exists(self.pipe_name)
        logging.info('clean pipe %s succ? %s' % (self.pipe_name, str(succ)))
        self.pipe_record_log.create_project()
        return succ

    def append(self, channel_id_iter):
        self._insert_channel_ids(channel_id_iter) 

    def reset(self, channel_id_iter):
        self.clean()
        self.append(channel_id_iter) 

    def watch_length(self, interval_secs=60):
        wait_time = 0
        wait_time_max = 3
        while True:
            current_length = self.get_length()
            logging.info('Watching %s, current lenght: %s' % (self.pipe_name, current_length))
            time.sleep(interval_secs)
            if current_length == 0:
                wait_time += 1
                if wait_time > wait_time_max:
                    logging.info('Watch finish, exit.')
                    break
            else:
                wait_time = 0

    def _iter_channel_ids(self, max_wait_time): 
        batch_size = 100
        total_num = 0
        wait_time = 0
        consumer_name, consumer_log_path = self.pipe_record_log.add_consumer()
        pop_id_list = []
        try:
            while True:
                channel_id_bytes = redis_spider.lpop(self.pipe_name)
                if channel_id_bytes is None:
                    wait_time += 1
                    if wait_time % 10 == 0:
                        logging.info('Waiting for producer to generate channel id. time: %d' % wait_time)
                    if wait_time > max_wait_time: 
                        logging.info('wait time reach max, exit.')
                        break
                    else:
                        time.sleep(1)
                else: 
                    wait_time = 0
                    channel_id = str(channel_id_bytes, encoding='utf-8')
                    yield channel_id
                    total_num += 1
                    pop_id_list.append(channel_id)
                    if total_num % batch_size == 0:
                        logging.info('get channel id from pipe. job name: %s, total: %d, currnet length: %d' 
                                % (consumer_name, total_num, self.get_length()))
                        self.pipe_record_log.dump_ids(consumer_log_path, pop_id_list)
                        pop_id_list.clear()
        finally:
            if pop_id_list:
                self.pipe_record_log.dump_ids(consumer_log_path, pop_id_list)
            if total_num > 0:
                self.pipe_record_log.update_consumer(consumer_name, total_num)
        
    def iter_items(self, max_wait_secs=60):
        for channel_id in self._iter_channel_ids(max_wait_secs):
            yield channel_id

 
def test_producer():
    from mysql_utils import iter_channel_ids
    sql = 'select cid from kol_channel_base order by sub desc limit 10'
    pipe_name = 'youtube_channel_id_pipe.test'
    pipe = ChannelIdPipe(pipe_name) 
    #pipe.clean()
    channel_id_iter = iter_channel_ids(sql)
    pipe.reset(channel_id_iter)
    print(pipe.get_length())

    #time.sleep(1)
    channel_id_iter = iter_channel_ids(sql)
    pipe.append(channel_id_iter)
    print(pipe.get_length())

    #time.sleep(1)
    channel_id_iter = iter_channel_ids(sql)
    pipe.append(channel_id_iter)
    print(pipe.get_length())
    #pipe.watch_pipe()


def test_consumer():
    pipe_name = 'youtube_channel_id_pipe.test'
    pipe = ChannelIdPipe(pipe_name) 
    for channel_id in pipe.iter_items(10):
        print(channel_id)


if __name__ == '__main__':
    test_producer()
    test_consumer()


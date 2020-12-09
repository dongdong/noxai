import redis
from mysql_utils import get_channel_data_rows
import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')
import time

pool_spider = redis.ConnectionPool(
        host='10.100.1.254', 
        password='dhmi5xMPjjfidUx4', 
        port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)

redis_channel_id_list_name = 'all_tag_rule_cid'
#redis_channel_id_list_name = 'all_tag_rule_cid.zh'
#redis_channel_id_list_name = 'all_tag_rule_cid.test'
#redis_channel_id_list_name = 'all_tag_rule_cid.other'
#redis_channel_id_list_name = 'all_tag_rule_cid.ko'


def clean_pipe():
    redis_spider.delete(redis_channel_id_list_name)
    succ = not redis_spider.exists(redis_channel_id_list_name)
    logging.info('clean pipe %s succ? %s' % (redis_channel_id_list_name, str(succ)))
    return succ


def get_current_length(redis_name):
    return redis_spider.llen(redis_name)


def watch_pipe_current_length(redis_name, interval=60):
    wait_time = 0
    wait_time_max = 10
    while True:
        current_length = get_current_length(redis_name)
        logging.info('Watch, list name: %s, current lenght: %s' % (redis_name, current_length))
        time.sleep(interval)
        if current_length == 0:
            wait_time += 1
            if wait_time > wait_time_max:
                logging.info('Watch finish, exit.')
                break
        else:
            wait_time = 0


def watch_pipe():
    watch_pipe_current_length(redis_channel_id_list_name)


def write_id_2_redis(redis_name, sql):
    total_num = 0
    batch_size = 1000
    for row in get_channel_data_rows(sql):
        channel_id = row[0]
        if len(channel_id) == 24 and channel_id.startswith('UC'):
            redis_spider.rpush(redis_name, channel_id)
            total_num += 1
        if total_num % batch_size == 0:
            length = get_current_length(redis_name)
            logging.info('writing channel id to redis... total: %d, currnet length: %d' % (total_num, length))
    logging.info('write channel id to redis finish! total: %d' % (total_num))


def get_sql():
    #sql = 'select cid from kol_channel_base where (lang="zh" or lang="zh-Hant" or lang="zh-Hans") order by sub desc'
    #sql = 'select cid from kol_channel_base where lang="ko" order by sub desc'
    #sql = 'select cid from kol_channel_base where sub > 50000'
    #sql = 'select cid from kol_channel_base where sub > 50000'
    #sql = 'select cid from kol_channel_base where sub <= 50000 and sub > 10000'
    sql = 'select cid from kol_channel_base where sub <= 10000 and sub > 5000'

    return sql


def insert_pipe():
    sql = get_sql()
    write_id_2_redis(redis_channel_id_list_name, sql)


def set_pipe():
    '''
    if clean_pipe():
        insert_pipe()
    else:
        logging.error('clean data error!')
    '''
    insert_pipe()

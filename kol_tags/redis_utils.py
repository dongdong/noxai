import redis
from mysql_utils import get_channel_data_rows
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s : %(message)s')
import time

pool_spider = redis.ConnectionPool(
        host='10.100.1.254', 
        password='dhmi5xMPjjfidUx4', 
        port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)

#redis_channel_id_list_name = 'all_tag_rule_cid'
redis_channel_id_list_name = 'all_tag_rule_cid.test'


def clean_data():
    redis_spider.delete(redis_channel_id_list_name)
    return not redis_spider.exists(redis_channel_id_list_name)


def get_current_length(redis_name):
    return redis_spider.llen(redis_name)


def watch_current_length(redis_name, interval=30):
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


def run_write_ids():
    #sql = 'select cid from kol_channel_base order by sub desc'
    #sql = 'select cid from kol_channel_base where lang = "zh" and sub > 10000 order by sub desc'
    #sql = 'select cid from kol_channel_base where lang = "zh-Hant" and sub > 10000 order by sub desc'
    sql = 'select cid from kol_channel_base where lang = "en" and sub > 10000 order by sub desc'
    #sql += ' limit 100'
    write_id_2_redis(redis_channel_id_list_name, sql)


def test_write_ids():
    #sql = 'select cid from kol_channel_base order by sub desc'
    #sql = 'select cid from kol_channel_base where lang = "zh" and sub > 10000 order by sub desc'
    #sql = 'select cid from kol_channel_base where lang = "zh-Hant" and sub > 10000 order by sub desc'
    sql = 'select cid from kol_channel_base where lang = "en" and sub > 10000 order by sub desc'
    sql += ' limit 100'
    logging.info('test write id. sql: %s' % sql)
    write_id_2_redis(redis_channel_id_list_name, sql)


def run_clean_and_write_ids():
    if clean_data():
        run_write_ids()
    else:
        logging.error('clean data error!')


if __name__ == '__main__':
    #run_write_ids()
    #test_write_ids()
    #clean_data()

    #print(get_current_length(redis_channel_id_list_name))
    #run_clean_and_write_ids()
    watch_current_length(redis_channel_id_list_name)



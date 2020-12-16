import pymysql
import mysql.connector
from elasticsearch import Elasticsearch, helpers
import redis
import logging
import time

es = Elasticsearch(['http://10.100.1.176:9200'], timeout=30)
channel_index = 'kol_v8'

kol_v2_config = {
     'user': 'uc_nox_kol',
     'password': 'cXNGUdJ5OI7KUleO',
     'host': '10.100.1.234',
     'database': 'nox_kol_v2',
     'raise_on_warnings': True,
     'charset': 'utf8mb4',
     'buffered': True,
}

spider_config = {
    'user': 'nox_kol_spider',
    'password': 'jt9bWbw8mwmtaze(mAax',
    'host': '10.100.1.85',
    'database': 'nox_kol_data',
    'raise_on_warnings': True,
    'charset': 'utf8mb4',
    'buffered': True,
}

pool_spider = redis.ConnectionPool(
        host='10.100.1.254', 
        password='dhmi5xMPjjfidUx4', 
        port=6379)
redis_spider = redis.Redis(connection_pool=pool_spider)

redis_channel_id_list_name = 'fans_age_gender_dist_cid'


def _get_rows_from_mysql(config, sql):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows


def _get_channel_data_rows(sql):
    cnx = mysql.connector.connect(**spider_config)
    cursor = cnx.cursor(pymysql.cursors.SSCursor)
    cursor.execute(sql)
    row = cursor.fetchone()
    while True:
        if row:
            yield row
            row = cursor.fetchone()
        else:
            break
    cursor.close()
    cnx.close()


def get_rows_by_sql_from_kol_v2(sql):
    rows = _get_rows_from_mysql(kol_v2_config, sql)
    return rows


def get_channel_contents_from_es(channel_id):
    source = es.get_source(index=channel_index, id=channel_id)
    return source


def update_channel_contents_to_es(channel_id, data):
    param = {"retry_on_conflict": 5}
    es.update(index=channel_index, params=param, id=channel_id, body={"doc": data})


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
    for row in _get_channel_data_rows(sql):
        channel_id = row[0]
        if len(channel_id) == 24 and channel_id.startswith('UC'):
            redis_spider.rpush(redis_name, channel_id)
            total_num += 1
        if total_num % batch_size == 0:
            length = get_current_length(redis_name)
            logging.info('writing channel id to redis... total: %d, currnet length: %d' 
                    % (total_num, length))
    logging.info('write channel id to redis finish! total: %d' % (total_num))


def get_sql():
    #sql = 'select cid from kol_channel_base where sub > 10000 limit 5000'
    #sql = 'select cid from kol_channel_base where sub > 10000'
    sql = 'select cid from kol_channel_base where lang="zh-Hant" or lang="zh-Hans" or lang="zh"'
    return sql


def insert_pipe():
    sql = get_sql()
    write_id_2_redis(redis_channel_id_list_name, sql)


def set_pipe():
    insert_pipe()


def reset_pipe():
    if clean_pipe():
        insert_pipe()
    else:
        logging.error('clean data error!')
    insert_pipe()


def pop_channel_id_from_pipe():
    wait_time = 0
    max_wait_time = 10
    while True:
        channel_id_bytes = redis_spider.lpop(redis_channel_id_list_name)
        if channel_id_bytes is None:
            wait_time += 1
            if wait_time % 10 == 0:
                logging.info('Waiting for producer to generate channel id. time: %d' 
                        % wait_time)
            if wait_time > max_wait_time: 
                logging.info('wait time reach max, exit.')
                break
            else:
                time.sleep(1)
                #continue
        else: 
            wait_time = 0
            channel_id = str(channel_id_bytes, encoding='utf-8')
            yield channel_id




import pymysql
import mysql.connector

kol_config = {
     'user': 'uc_nox_kol',
     'password': 'cXNGUdJ5OI7KUleO',
     'host': '10.100.1.234',
     'database': 'nox_kol_v2',
     'raise_on_warnings': True,
     'charset': 'utf8mb4',
     'buffered': True,
}

kol_data_config = {
    'user': 'nox_kol_spider',
    'password': 'jt9bWbw8mwmtaze(mAax',
    'host': '10.100.1.85',
    'database': 'nox_kol_data',
    'raise_on_warnings': True,
    'charset': 'utf8mb4',
    'buffered': True,
}

def _get_all_rows_from_mysql(config, sql):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows


def _iter_rows_from_mysql(config, sql):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(pymysql.cursors.SSCursor)
    cursor.execute(sql)
    while True:
        row = cursor.fetchone()
        if row:
            yield row
        else:
            break
    cursor.close()
    cnx.close()


def get_all_tag_info_rows():
    sql = 'select * from brand_tag_v3 where is_delete = 1'
    rows = _get_all_rows_from_mysql(kol_config, sql)
    return rows


def iter_channel_ids(sql): 
    for row in _iter_rows_from_mysql(kol_data_config, sql):
        channel_id = row[0]
        if len(channel_id) == 24 and channel_id.startswith('UC'):
            yield channel_id


def get_origin_real_data_iter():
    sql = 'select * from ytb_analytics_data_origin_real_data'
    data_iter = _iter_rows_from_mysql(kol_config, sql)
    return data_iter

def test_channel_data():
    sql = 'select cid from kol_channel_base order by sub desc limit 10'
    for channel_id in iter_channel_ids(sql):
        print(row)
    

if __name__ == '__main__':
    test_channel_data()



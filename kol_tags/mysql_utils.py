import pymysql
import mysql.connector

config = {
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

def get_tag_info_rows():
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    #cursor.execute('select parent_id,' + query_field + ' from brand_tag_v2 where id=%s', (tag_id,))
    cursor.execute('select * from brand_tag_v2')
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows


def get_channel_data_rows(sql):
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


def test():
    tag_info_list = get_tag_info_rows()
    for tag_info in tag_info_list:
        print(tag_info)


def test_channel_data():
    sql = 'select * from kol_channel_base order by sub desc limit 10'
    for row in get_channel_data_rows(sql):
        print(row)
    

if __name__ == '__main__':
    #test()
    test_channel_data()



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


def _get_tag_info_rows(sql):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    #cursor.execute('select parent_id,' + query_field + ' from brand_tag_v2 where id=%s', (tag_id,))
    #cursor.execute('select * from brand_tag_v2')
    #cursor.execute('select * from brand_tag_v2_bak')
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows


def get_tag_info_rows():
    sql = 'select * from brand_tag_v3 where is_delete = 1'
    rows = _get_tag_info_rows(sql)
    return rows


def _write_tag_info_rows(table_name, rows):
    sql = 'insert into %s values %s' % (table_name, ', '.join([str(row) for row in rows]))
    sql = sql.replace('None', 'NULL')
    #print(sql)

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(sql)
        cnx.commit()
    except:
        print('Exception occurs when writing tag info to mysql, rollback.')
        cnx.rollback()
    cursor.close()
    cnx.close()


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


def update_brand_tag():
    from tag_config import structured_tag_config_en
    en_name_id_map = {}
    sql = 'select * from brand_tag_v2_bak'
    new_table_name = 'brand_tag_v3'
    rows = _get_tag_info_rows(sql)

    for row in rows:
        _id, parent_id, en_name, zh_name, is_delete, ko_name = row
        #_id, en_name = row[0], row[2]
        if en_name and is_delete == 1:
            if en_name in en_name_id_map:
                print('duplicate en_name:', en_name)
            en_name_id_map[en_name] = _id
        else:
            pass

    new_id_parent_id_map = {}
    parent_id_set = set()
    id_priority_map = {}
    cur_priority = 0
    level_1_count = 0
    for level_1_en_name, level_2_en_name_list in structured_tag_config_en.items():
        if level_1_en_name in en_name_id_map:
            parent_id = en_name_id_map[level_1_en_name]
            parent_id_set.add(parent_id)
            cur_priority = level_1_count * 100 + 1000
            level_1_count += 1
            id_priority_map[parent_id] = cur_priority
            for level_2_en_name in level_2_en_name_list:
                if level_2_en_name in en_name_id_map:
                    child_id = en_name_id_map[level_2_en_name] 
                    new_id_parent_id_map[child_id] = parent_id
                    cur_priority += 1
                    id_priority_map[child_id] = cur_priority
                else:
                    print('level 2 name not in database', level_2_en_name)
        else:
           print('level 1 name not in database', level_1_en_name)

    default_priority = 10000
    new_rows = []
    for row in rows:
        _id, parent_id, en_name, zh_name, is_delete, ko_name = row
        display_priority = id_priority_map.get(_id, default_priority)
        if _id in new_id_parent_id_map:
            new_row = (_id, new_id_parent_id_map[_id], en_name, zh_name, is_delete, ko_name, display_priority)
        elif _id in parent_id_set:
            new_row = (_id, None, en_name, zh_name, is_delete, ko_name, display_priority)
        else:
            new_row = (_id, None, en_name, zh_name, 2, ko_name, None)
        new_rows.append(new_row)

    #new_rows = sorted([row for row in new_rows if row[4] == 1], key=lambda x: x[6])
    #for row in new_rows:
    #    print(row)

    _write_tag_info_rows(new_table_name, new_rows)


def test():
    #sql = 'select * from brand_tag_v3 where is_delete = 1 order by display_priority'
    #sql = 'select * from brand_tag_v3'
    sql = 'select * from brand_tag_v3 where is_delete = 1 and ch_name != "" order by display_priority'
    tag_info_list = _get_tag_info_rows(sql)
    for tag_info in tag_info_list:
        #print(tag_info)
        print(tag_info[0], tag_info[2])


def test_channel_data():
    sql = 'select * from kol_channel_base order by sub desc limit 10'
    for row in get_channel_data_rows(sql):
        print(row)
    

if __name__ == '__main__':
    test()
    #test_channel_data()
    #update_brand_tag()


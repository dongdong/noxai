# -*- coding: utf-8 -*-
# @Time    : 2020/6/30 19:34
# @Author  : Jexy
# @Email   : gongxingyao@bignox.com
# @File    : dongdongExport.py
# @Software: PyCharm

import pymysql
import mysql.connector
import xlsxwriter
import time
import sys

key_values = {"NoxInfluencer": "NoxInfluencer",
              "NoxPlayer": "player",
              "NoxCleaner": "cleaner",
              "NoxBrowser": "Browser",
              "NoxFileManager": "FileManager",
              "NoxSecurity": "Security",
              "Cellular Corps": "corps",
              "Key Point": "point",
              "Dot n Beat": "beat",
              "Egg finder": "finder",
              "iFun Downloader": "Downloader",
              "idle car tycoon": "tycoon",
              "Kaka Share": "Kaka",
              "NoxAppLock": "appLock",
              "WaterTime": "WaterTime",
              "NoxMemory": "NoxMemory",
              "NoxStep": "NoxStep",
              "Lucky Wallpaper": "Wallpaper",
              "NoxOcean": "NoxOcean",
              "Sleep Theory": "Theory",
              "Bloom": "Bloom"}

online_config = {
     'user': 'uc_nox_kol',
     'password': 'cXNGUdJ5OI7KUleO',
     'host': '10.100.1.234',
     'database': 'nox_kol_v2',
     'raise_on_warnings': True,
     'charset': 'utf8mb4',
     'buffered': True,
}

sql = """
select vmp.project_name,
       vmt.star_url,
       vmt.fans_number,
       vmt.cost,
       vmt.video_url,
       vmt.video_publish_time,
       vmt.video_total_views,
       vmt.likes,
       vmt.comments_number,
       vmt.short_link,
       vmt.platform_id,
       vmt.sl_clicks,
       vmt.sl_clicks_pv,
       (select vmts1.video_views from video_monitor_task_snapshot vmts1 where vmts1.task_id = vmt.id and vmts1.interval_no=3) days3Views,
       (select vmts2.likes from video_monitor_task_snapshot vmts2 where vmts2.task_id = vmt.id and vmts2.interval_no=3) days3Likes,
       (select vmts3.comments_number from video_monitor_task_snapshot vmts3 where vmts3.task_id = vmt.id and vmts3.interval_no=3) days3CommentNumber,
       (select vmts4.video_views from video_monitor_task_snapshot vmts4 where vmts4.task_id = vmt.id and vmts4.interval_no=7) days7Views,
       (select vmts5.likes from video_monitor_task_snapshot vmts5 where vmts5.task_id = vmt.id and vmts5.interval_no=7) days7Likes,
       (select vmts6.comments_number from video_monitor_task_snapshot vmts6 where vmts6.task_id = vmt.id and vmts6.interval_no=7) days7CommentNumber
from video_monitor_project vmp
       left join video_monitor_project_task_relation vmptr on vmp.id = vmptr.project_id
       left join video_monitor_task vmt on vmt.id = vmptr.task_id
where vmt.status!=6 and project_name like '%%%s%%'
"""

workbook = xlsxwriter.Workbook("./nox_product_promote_data.xlsx")
for k, v in key_values.items():
    print(k)
    worksheet = workbook.add_worksheet(k)
    worksheet.write_string(0, 0, "推广名称")
    worksheet.write_string(0, 1, "网红地址")
    worksheet.write_string(0, 2, "粉丝量")
    worksheet.write_string(0, 3, "花费")
    worksheet.write_string(0, 4, "视频地址")
    worksheet.write_string(0, 5, "视频发布时间")
    worksheet.write_string(0, 6, "总观看量")
    worksheet.write_string(0, 7, "点赞量")
    worksheet.write_string(0, 8, "评论数")
    worksheet.write_string(0, 9, "追踪链接")
    worksheet.write_string(0, 10, "平台")
    worksheet.write_string(0, 11, "链接UV")
    worksheet.write_string(0, 12, "链接PV")
    worksheet.write_string(0, 13, "3天观看量")
    worksheet.write_string(0, 14, "3天点赞量")
    worksheet.write_string(0, 15, "3天评论量")
    worksheet.write_string(0, 16, "7天观看量")
    worksheet.write_string(0, 17, "7天点赞量")
    worksheet.write_string(0, 18, "7天评论量")
    #conn = pymysql.connect(**online_config)
    conn = mysql.connector.connect(**online_config)
    cursor = conn.cursor()
    cursor.execute(sql % v)
    for index, row in enumerate(cursor.fetchall()):
        worksheet.write(index + 1, 0, row[0])
        worksheet.write(index + 1, 1, row[1])
        worksheet.write(index + 1, 2, row[2])
        worksheet.write(index + 1, 3, row[3])
        worksheet.write(index + 1, 4, row[4])
        if row[5]:
            worksheet.write(index + 1, 5, str(row[5]))
        worksheet.write(index + 1, 6, row[6])
        worksheet.write(index + 1, 7, row[7])
        worksheet.write(index + 1, 8, row[8])
        worksheet.write(index + 1, 9, row[9])
        worksheet.write(index + 1, 10, row[10])
        worksheet.write(index + 1, 11, row[11])
        worksheet.write(index + 1, 12, row[12])
        worksheet.write(index + 1, 13, row[13])
        worksheet.write(index + 1, 14, row[14])
        worksheet.write(index + 1, 15, row[15])
        worksheet.write(index + 1, 16, row[16])
        worksheet.write(index + 1, 17, row[17])
        worksheet.write(index + 1, 18, row[17])
workbook.close()



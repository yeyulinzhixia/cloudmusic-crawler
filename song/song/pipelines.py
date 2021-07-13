'''
Descripttion: 
version: 
Author: yeyu
Date: 2021-01-29 16:24:20
LastEditors: yeyu
LastEditTime: 2021-04-20 14:09:54
'''
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql
from scrapy.exceptions import DropItem
class SongPipeline:
    def open_spider(self,spider):
        self.connection = pymysql.connect(host='localhost', port=3306, user='root', password='Wxl200825',db='netease',charset='utf8')
        self.cursor = self.connection.cursor()
    def process_item(self, item, spider):
        data =list(dict(item).values())
        insert1 = ('%s,'*len(data))[:-1]
        insert2  = ",".join(dict(item).keys())
        sql = 'INSERT INTO song2 ('+insert2+') VALUES ('+insert1+')'
        try:
            self.cursor.execute(sql, data)
            self.connection.commit()
            return item
        except:
            raise DropItem("重复item")
    def close_spider(self,spider):
        print(spider.crawler.stats.get_stats())
        self.cursor.close()
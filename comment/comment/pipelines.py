# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
class CommentPipeline:
    def process_item(self, item, spider):
        return item

from .settings import MYSQL_CONFIG
import pymysql
from scrapy.exceptions import DropItem
class MysqlPipeline:
    def open_spider(self,spider):
        self.connection = pymysql.connect(host=MYSQL_CONFIG['host'], port=MYSQL_CONFIG['port'], user=MYSQL_CONFIG['user'], password=MYSQL_CONFIG['password'],db=MYSQL_CONFIG['db'],charset=MYSQL_CONFIG['charset'])
        self.cursor = self.connection.cursor()
    def process_item(self, item, spider):
        data =list(dict(item).values())
        insert1 = ('%s,'*len(data))[:-1]
        insert2  = ",".join([ "`"+i+"`" for i  in dict(item).keys()])
        sql = 'INSERT INTO video_stat ('+insert2+') VALUES ('+insert1+')'
        try:
            self.cursor.execute(sql, data)
            self.connection.commit()
            return item
        except:
            raise DropItem("重复item")
    def close_spider(self,spider):
        print(spider.crawler.stats.get_stats())
        self.cursor.close()

import pymongo
from .settings import MONGO_CONFIG
import time
class MongoPipeline:
    def process_item(self, item, spider):
        client = pymongo.MongoClient(MONGO_CONFIG['url'])
        db = client[MONGO_CONFIG['db']]
        col = db[MONGO_CONFIG['col']]
        data = item['data']
        data['_id'] = data['commentId']
        try:
            col.insert_one(data)
            return item
        except Exception as e:
            raise DropItem("重复item")
import scrapy
from ..settings import MYSQL_CONFIG
import pymysql
import time
import json
from ..items import CommentItem
import logging
class GetallSpider(scrapy.Spider):
    name = 'getall'
    allowed_domains = ['163.com']
    start_urls = ['http://163.com/']

    def start_requests(self):
        connection = pymysql.connect(host=MYSQL_CONFIG['host'], port=MYSQL_CONFIG['port'], user=MYSQL_CONFIG['user'], password=MYSQL_CONFIG['password'],db=MYSQL_CONFIG['db'],charset=MYSQL_CONFIG['charset'])
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM song2 ORDER BY RAND() LIMIT 200')
        result = cursor.fetchall()
        ids = [i[0] for i in list(result)]
        for i in ids:
            print('==================',i)
            URL  = 'https://music.163.com/api/comment/resource/comments/get'
            query ={"rid":"R_SO_4_"+str(i),"threadId":"R_SO_4_"+str(i),"pageNo":"1","pageSize":"1000","cursor":"-1","offset":"0","orderType":"1","csrf_token":""}
            yield scrapy.FormRequest(URL,formdata = query,headers={'referer':'https://music.163.com/song?id='+str(i)},meta={'pageNo':1,'id':i})

    def parse(self, response):
        id = response.meta['id']
        page = response.meta['pageNo']
        data = json.loads(response.text)
        
        if 'data' in data.keys():

            if len(data['data']['comments']):
                print('=========page========',page)
                page += 1
                URL  = 'https://music.163.com/api/comment/resource/comments/get'
                query ={"rid":"R_SO_4_"+str(id),"threadId":"R_SO_4_"+str(id),"pageNo":str(page),"pageSize":"1000","cursor":data['data']['cursor'],"offset":"0","orderType":"1","csrf_token":""}
                yield scrapy.FormRequest(URL,formdata = query,headers={'referer':'https://music.163.com/song?id='+str(id)},meta={'pageNo':page,'id':id},callback=self.parse)

            for i in data['data']['comments']:
                item = CommentItem()
                item['data'] = i
                item['data']['userId'] = item['data']['user']['userId']
                item['data']['musicId'] = id
                del item['data']['user']
                if item['data']['beReplied']!=None:
                    item['data']['beReplied'] =item['data']['beReplied'][0]['beRepliedCommentId']
                yield item
        else:
            print(response.text)



from pymongo.results import InsertOneResult
import scrapy
from song.items import Song2Item
import json
import execjs
import logging
import pymongo
import datetime
class OnlineSpider(scrapy.Spider):
    name = 'online'
    allowed_domains = ['163.com']
    js = open('/root/Music163.js', 'r').read()
    ext = execjs.compile(js)

    def __init__(self,pages=80, *args, **kwargs):
        super(eval(self.__class__.__name__), self).__init__(*args, **kwargs)
        self.pages = int(pages)

    def start_requests(self):
        #记录日志信息
        self.crawler.stats.set_value('pages',self.pages)

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["neteaselog"]
        mycol = mydb["crawlrate"]
        try:
            start = [i for i in mycol.find().sort("start_id",-1).limit(1)][0]['start_id']+1000
        except:
            # 第一次启动
            start = 1860567964
        self.crawler.stats.set_value('start_id',start)
        for _ in range(self.pages):
            s = start
            e = start+1000
            ids = [i for i in range(s,e)]
            query = {"c":str([{'id':i} for i in ids]),'ids':str(ids)}
            param = self.ext.call('start',query)
            url = 'https://music.163.com/weapi/v3/song/detail'
            yield scrapy.FormRequest(url,formdata = param,headers={'Referer':'https://music.163.com/'},meta={'start_id':s})
            start+=1000
        self.crawler.stats.set_value('end_id',e)

    def parse(self, response):
        # check status code
        count = 0
        if (json.loads(response.text))['code']==200:    
            if 'songs' in json.loads(response.text).keys():
                for data in (json.loads(response.text))['songs']:
                    try:
                        item = Song2Item()
                        for i in item.fields.keys():
                            if i=='ar':
                                if len(data[i])!=0 and data[i]!=None:
                                    item[i] = '\n'.join([j['name']+','+str(j['id']) for j in data[i]])
                            elif i.startswith('al_'):
                                if data['al'][i[3:]]!=None:
                                    item[i] = data['al'][i[3:]]
                            elif i in data.keys():
                                if type(data[i])==list:
                                    if len(data[i])!=0:
                                        item[i] = ','.join(data[i])
                                else:
                                    if data[i]!=None:
                                        item[i] = data[i]
                        if item['name'] !='':
                            yield item
                            count += 1
                    except:
                        pass
            myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            mydb = myclient["neteaselog"]
            mycol = mydb["crawlrate"]
            info =  {'start_id':response.meta['start_id'],'rate':count/1000,'crawltime':datetime.datetime.now()}
            mycol.insert_one(info)
            logging.info(f'爬取率：{count/1000}')
        else:
            logging.warning('IP已禁用')

import scrapy
import scrapy
from song.items import Song2Item
import json
import execjs
import logging
import pymongo
import pandas as pd
import datetime
from scrapy.utils.project import get_project_settings

class ResearchSpider(scrapy.Spider):
    name = 'research'
    allowed_domains = ['163.com']
    js = open('/root/Music163.js', 'r').read()
    ext = execjs.compile(js)

    def __init__(self,day=1,rate=0.2,pages = 10, *args, **kwargs):
        super(eval(self.__class__.__name__), self).__init__(*args, **kwargs)
        self.day = float(day)
        self.rate = float(rate)
        self.pages = int(pages)


    def start_requests(self):
        #记录日志信息
        self.crawler.stats.set_value('day',self.day)
        self.crawler.stats.set_value('rate',self.rate)
        self.crawler.stats.set_value('pages',self.pages)
        #获取列表
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["neteaselog"]
        mycol = mydb["crawlrate"]
        data =  [i for i in mycol.find()]
        a = pd.DataFrame()
        
        a['start_id'] = [i['start_id'] for i in data]
        a['end_id'] = [i['start_id']+1000 for i in data]
        a['rate'] =  [i['rate'] for i in data]
        a['crawltime'] = [i['crawltime'] for i in data]
        a['time_cha'] = a['crawltime'].map(lambda x:(datetime.datetime.now()-x).days)

        result = a[(a['time_cha']<self.day) & (a['rate']>0.1) &(a['rate']<self.rate)].sample(self.pages)
        for i in zip(result['start_id'], result['end_id']):
            s = int(i[0])
            e = int(i[1])
            self.crawler.stats.set_value('start_id',s)
            self.crawler.stats.set_value('end_id',e)
            ids = [i for i in range(s,e)]
            query = {"c":str([{'id':i} for i in ids]),'ids':str(ids)}
            param = self.ext.call('start',query)
            url = 'https://music.163.com/weapi/v3/song/detail'
            logging.info(f'当前从{s}到{e}')
            yield scrapy.FormRequest(url,formdata = param,headers={'Referer':'https://music.163.com/'},meta={'start_id':s})

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
            mycol.update_one({"start_id": response.meta['start_id']}, {"$set": info})
            logging.info(f'爬取率：{count/1000}')
        else:
            logging.warning('IP已禁用')
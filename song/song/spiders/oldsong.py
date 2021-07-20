import scrapy
from song.items import MongoItem
import json
import execjs
import logging
import pymongo
import datetime

class OldsongSpider(scrapy.Spider):
    name = 'oldsong'
    allowed_domains = ['163.com']
    custom_settings = {
        'ITEM_PIPELINES': {
            'song.pipelines.MongoPipeline': 400
        }
    }
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
            start = [i for i in mycol.find({'spider':"song.oldsong"}).sort("start_id",-1).limit(1)][0]['start_id']+1000
        except:
            # 第一次启动
            start = 100000
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
                        item = MongoItem()
                        if data['name'] != '':
                            item['data'] = data
                            yield item
                            count += 1
                    except Exception as e:
                        print(e)
                myclient = pymongo.MongoClient("mongodb://localhost:27017/")
                mydb = myclient["neteaselog"]
                mycol = mydb["crawlrate"]
                info =  {'start_id':response.meta['start_id'],'rate':count/1000,'crawltime':datetime.datetime.now(),'spider':"song.oldsong"}
                mycol.insert_one(info)
                logging.info(f'爬取率：{count/1000}')
            else:
                logging.warning(f'错误信息：{response.text}')
        else:
            logging.warning('IP已禁用')

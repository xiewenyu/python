'''
Created on 2015年9月3日
爬虫：www.boohee.com食品数据
@author: xiewenyu
'''
# -*- coding:utf-8 -*-

import urllib.request
import re
import pymysql.cursors
 
conn = pymysql.connect(host='localhost',
        user='root',
        passwd='root',
        db='ucontent',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
#抓取MM
class Spider:
 
    #页面初始化
    def __init__(self):
        self.siteURL = 'http://www.boohee.com/food/group/'
        self.insertSQL="INSERT INTO t_food(category,name,calories,unit) VALUES (%s,'%s','%s','%s') "

 
    #获取索引页面的内容
    def getPage(self,groupId,pageIndex):
        pageUrl = self.siteURL +str(groupId)+"?page=" + str(pageIndex)
        req = urllib.request.Request(pageUrl)
        response = urllib.request.urlopen(req)
        return response.read().decode('utf-8')

    def getName(self,item):
        item = item.replace("，",",");  
        pattern=re.compile('<a.*?title="([^,]+).*?".*?target.*?class="gray1".+',re.S)
        items = re.findall(pattern,item)
        return items[0]
    
    def getCalories(self,item):
        #pattern=re.compile('<p.*?class="gray2[^：]*?：[^(](.*?)\(([^)].*?)\)</p>', re.S)
        pattern=re.compile('<p.*?class="gray2[^：]*?：(.*?)\(([^)].*?)\)</p>', re.S)
        items = re.findall(pattern,item)
        return items[0]
            
    #获取页面所有数据项目
    def getContents(self,grouopId,pageIndex):
        page = self.getPage(grouopId,pageIndex)
        pattern = re.compile('<div class="intr.*?float-right.*?</div>',re.S)
        items = re.findall(pattern,page)
        contents = []
        for item in items:
            name=self.getName(item)
            value=self.getCalories(item)
            contents.append([name,value[0],value[1]])
        return contents
 

    def savePageInfo(self,grouopId):
        try:
                for pageIndex in range(1,11):
                    contents = self.getContents(grouopId,pageIndex)
                    for item in contents:
                        with conn.cursor() as cursor:
                            #print(item)
                            insert=self.insertSQL%(grouopId,item[0],item[1],item[2])
                            print(insert)
                            cursor.execute(insert)
                            conn.commit()
        except Exception as err:
            print("insert error:"+str(err.message))
#         finally:
#             self.conn.close()
             
             
            
 
    #传入group range
    def savePagesInfo(self,start,end):
        for groupId in range(start,end+1):
            self.savePageInfo(groupId)
 
spider = Spider()
spider.savePagesInfo(1,10)

print("end---------------------")
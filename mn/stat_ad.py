'''
Created on 2015年5月18日

@author: xiewenyu
'''
# -*- coding: utf-8 -*-
import re
import os
import gzip
import datetime
import pymysql.cursors

connection = pymysql.connect(host='42.62.42.202',
                         user='enwang',
                         passwd='enwang2014PaSsW0rd',
                         port=3306,
                         db='union-portal',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    
ipP=r"(?P<ip>\d{1,3}[.]\d{1,3}[.]\d{1,3}[.]\d{1,3})"
timeP=r"\[(?P<time>[^]\s]+).+?\]"
requestP=r"[^/]+(?P<request>/\w+/(js|h)/\w+\.(?=js|html)(js|html))\s[^\s]+\s(?!404)"
#requestP=r"[^/]+(?P<request>/\w+/(js|h)/\w+\.(?=js|html)(js|html)).*"
pattern=re.compile(r"%s\s?-\s?-\s?%s\s?%s" %(ipP,timeP,requestP),re.IGNORECASE|re.VERBOSE)

def read_gz_file(path):
    if os.path.exists(path):
        with gzip.open(path, 'rt') as pf:
            for line in pf:
                yield line
    else:
        print('the path [{}] is not exist!'.format(path))

def analyzing():
    request_pv_map={};
    request_ip_map={};
    yesterday = get_yesterday()
    log = read_gz_file('F:/ads_log/access.log-'+yesterday.strftime('%Y%m%d')+'.gz')
    if getattr(log, '__iter__', None):
        for line in log:
            matcher=pattern.match(line)
            #print(line)
            if(matcher is None):
                #print("--None:",line)
                continue
            else:
                ip=matcher.group("ip")
                time=datetime.datetime.strptime(matcher.group("time"),"%d/%b/%Y:%H:%M:%S");
                request=matcher.group("request")
                
                #pv
                if(request_pv_map.get(request) != None):
                    request_pv_map[request]=request_pv_map.get(request)+1
                else:
                    request_pv_map[request]=1
                    
                #ip
                if(request_ip_map.get(request) != None):
                    ip_map=request_ip_map.get(request)
                    if(ip_map.get(ip) != None):
                        ip_map[ip]=ip_map.get(ip)+1
                    else:
                        ip_map[ip]=1
                else:
                    request_ip_map[request]={ip:1}
                #print(matcher.group(0))
                #print("--Found:%s--%s--%s"%(ip,time,request))
    #for (k,v) in request_ip_map.items():
        #print (k+" ip:"+str(len(v))+" , pv:"+str(request_pv_map.get(k)))
    saveLog(request_ip_map, request_pv_map)
# get yesterday    
def get_yesterday():
    return datetime.date.today() - datetime.timedelta(days=1)
# save to db
def saveLog(ip_map,pv_map):
    yesterday = get_yesterday().strftime('%Y%m%d')
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO t_channel_ad_log (log_date,channel_id,ad_uri,ip,view_times) VALUES (%s,%s,%s,%s,%s)"
            for (k,v) in ip_map.items():
                reqs=k.split("/")
                cursor.execute(sql, (yesterday,reqs[1],k,len(v),pv_map.get(k)))
                connection.commit()
    except Exception as err:
        print("error:",err)
    finally:
        connection.close()
        
if __name__ == '__main__':
    analyzing()
    print("---统计广告日志完毕---")
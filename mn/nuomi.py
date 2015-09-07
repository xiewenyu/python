# -*- coding: utf-8 -*-

'''
Created on 2015年7月10日

@author: xyuser
@see: 同步糯米网数据
'''
import urllib.request
import xml.dom.minidom
import sys
import pymysql.cursors
from pymysql.err import IntegrityError
import logging.handlers

_log = logging.getLogger()
_log.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler('nuomi_py.log', maxBytes=20*1024*1024, backupCount=100)
handler.setFormatter(logging.Formatter('%(levelname)s[%(asctime)s]:%(message)s',datefmt='%Y-%m-%d %H:%M:%S'))
#日志文件
_log.addHandler(handler)
#控制台
_log.addHandler(logging.StreamHandler());


citys=['aba', 'akesu', 'alashan', 'ali', 'altay', 'anji', 'ankang', 'anqing', 'anqiu', 'anshan', 'anshun', 'anyang', 'aomen', 'artux', 'baicheng', 'baise', 'baisha', 'baishan', 'baiyin', 'baoding', 'baoji', 'baoshan', 'baoting', 'baotou', 'bayangolmongol', 'bayannaoer', 'bazhong', 'beihai', 'beijing', 'bengbu', 'benxi', 'bijie', 'binzhou', 'boertalamongol', 'bozhou', 'cangzhou', 'changchun', 'changde', 'changdu', 'changji', 'changjiang', 'changsha', 'changshu', 'changxing', 'changzhi', 'changzhou', 'chaohu', 'chaoyang', 'chaozhou', 'chengde', 'chengdu', 'chengmai', 'chenzhou', 'chifeng', 'chizhou', 'chongqing', 'chongzuo', 'chunan', 'chuxiong', 'chuzhou', 'cixi', 'dafeng', 'daidehongjingpo', 'dali', 'dalian', 'dandong', 'danzhou', 'daqing', 'datong', 'daxinganling', 'dazhou', 'dengfeng', 'deqing', 'deyang', 'dezhou', 'dingan', 'dingxi', 'diqing', 'dongfang', 'dongguan', 'dongyang', 'dongying', 'dunhua', 'eerduosi', 'emeishan', 'enshi', 'ezhou', 'fangchenggang', 'fenghuang', 'foshan', 'fuqing', 'fushun', 'fuxin', 'fuyang', 'fuyang1', 'fuzhou', 'fuzhou1', 'gannan', 'ganzhou', 'ganzizhou', 'gongyi', 'guangan', 'guangyuan', 'guangzhou', 'guigang', 'guilin', 'guiyang', 'guoluo', 'guyuan', 'haerbin', 'haian', 'haibei', 'haidong', 'haikou', 'haimen', 'hainantibetan', 'haining', 'haixi', 'hami', 'handan', 'hangzhou', 'hanzhong', 'hebi', 'hechi', 'hefei', 'hegang', 'heihe', 'hengshui', 'hengyang', 'hetian', 'heyuan', 'heze', 'hezhou', 'hohhot', 'honghe', 'huadian', 'huaian', 'huaibei', 'huaihua', 'huainan', 'huanggang', 'huangnan', 'huangshan', 'huangshi', 'huizhou', 'huludao', 'hulunbeier', 'huzhou', 'jiamusi', 'jian', 'jiande', 'jiangmen', 'jiangyin', 'jiaozuo', 'jiashan', 'jiaxing', 'jiayuguan', 'jieshou', 'jieyang', 'jilin', 'jinan', 'jinchang', 'jincheng', 'jindezhen', 'jingjiang', 'jingmen', 'jingzhou', 'jinhua', 'jining', 'jinjiang', 'jintan', 'jinzhong', 'jinzhou', 'jiujiang', 'jiuquan', 'jiuzhaigou', 'jixi', 'jiyuan', 'kaifeng', 'kashgar', 'kelamayi', 'kunming', 'kunshan', 'laibin', 'laiwu', 'langfang', 'lanxi', 'lanzhou', 'lasa', 'ledong', 'leqing', 'leshan', 'liangshan', 'lianyungang', 'liaocheng', 'liaoning', 'liaoyang', 'liaoyuan', 'lijiang', 'linan', 'lincang', 'linfen', 'lingao', 'lingshui', 'linhai', 'lintong', 'linxia', 'linxiaa', 'linyi', 'lishui', 'liuan', 'liupanshui', 'liuzhou', 'liyang', 'longkou', 'longnan', 'longsheng', 'longyan', 'loudi', 'luohe', 'luoyang', 'luzhou', 'lvliang', 'maanshan', 'manzhouli', 'maoming', 'meishan', 'meizhou', 'mianyang', 'mingguang', 'mudanjiang', 'nanchang', 'nanchong', 'nanjing', 'nanning', 'nanping', 'nantong', 'nanyang', 'naqu', 'neijiang', 'ningbo', 'ningde', 'ningguo', 'nujianglisuzu', 'nyingchi', 'panjin', 'panzhihua', 'pingdingshan', 'pinghu', 'pingliang', 'pingxiang', 'puer', 'putian', 'puyang', 'qiandongnanmiaodongautonomous', 'qiannan', 'qianxinan', 'qidong', 'qingdao', 'qingyang', 'qingyuan', 'qingzhou', 'qinhuangdao', 'qinzhou', 'qionghai', 'qiongzhong', 'qiqihar', 'qitaihe', 'qizhou', 'quanzhou', 'qufu', 'qujing', 'quzhou', 'rikaze', 'rizhao', 'rugao', 'ruian', 'sanhe', 'sanmenxia', 'sanming', 'sanya', 'shanghai', 'shangluo', 'shangqiu', 'shangrao', 'shangyu', 'shannan', 'shantou', 'shanwei', 'shaoguan', 'shaoxing', 'shaoyang', 'shenyang', 'shenzhen', 'shihezi', 'shijiazhuang', 'shishi', 'shiyan', 'shizuishan', 'shouguang', 'shuangyashan', 'shunde', 'shuozhou', 'siping', 'songyuan', 'suihua', 'suining', 'suizhou', 'suqian', 'suzhou', 'suzhou1', 'tacheng', 'taian', 'taicang', 'taiyuan', 'taizhou', 'taizhoux', 'tanggu', 'tangshan', 'tianchang', 'tianjin', 'tianshui', 'tieling', 'tongcheng', 'tongchuan', 'tonghua', 'tongliao', 'tongling', 'tonglu', 'tongren', 'tongxiang', 'tulufan', 'tunchang', 'wanning', 'weifang', 'weihai', 'weinan', 'wenchang', 'wenling', 'wenshan', 'wenzhou', 'wuhai', 'wuhan', 'wuhu', 'wujiang', 'wulanchabu', 'wulumuqi', 'wuwei', 'wuxi', 'wuyishan', 'wuyuan', 'wuzhen', 'wuzhishan', 'wuzhong', 'wuzhou', 'xiamen', 'xian', 'xiangfan', 'xianggang', 'xiangshan', 'xiangtan', 'xiangxi', 'xiangyang', 'xianning', 'xiantao', 'xianyang', 'xiaogan', 'xiaoshan', 'xichang', 'xilinguole', 'xingan', 'xingtai', 'xingyang', 'xining', 'xinmi', 'xinxiang', 'xinyang', 'xinyu', 'xinzheng', 'xishuangbanna', 'xuancheng', 'xuchang', 'xuzhou', 'yaan', 'yanan', 'yanbian', 'yancheng', 'yangjiang', 'yangquan', 'yangshuo', 'yangzhou', 'yanji', 'yanliang', 'yantai', 'yanzhou', 'yibin', 'yichang', 'yichun', 'yichun1', 'yili', 'yinchuan', 'yingkou', 'yingtan', 'yiwu', 'yixing', 'yiyang', 'yizheng', 'yongkang', 'yongzhou', 'yueyang', 'yuhang', 'yulin', 'yulin1', 'yuncheng', 'yunfu', 'yushu', 'yuxi', 'yuyao', 'zaozhuang', 'zhangjiagang', 'zhangjiajie', 'zhangjiakou', 'zhangye', 'zhangzhou', 'zhanjiang', 'zhaoqing', 'zhaotong', 'zhengzhou', 'zhenjiang', 'zhongshan', 'zhongwei', 'zhoukou', 'zhoushan', 'zhuhai', 'zhuji', 'zhumadian', 'zhuozhou', 'zhuzhou', 'zibo', 'zigong', 'ziyang', 'zunyi']
api_url = "http://api.nuomi.com/api/dailydeal?version=v1&city="

dbcon = pymysql.connect(host='localhost',
                         user='root',
                         passwd='root',
                         port=3308,
                         db='news',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)

sql="INSERT INTO t_nuomi(id,city,city_pinyin,loc,title,img,first_category,second_category,start_time,end_time,value,price,rebate"
sql+=",s_name,s_tel,s_addr,s_area,s_longitude,s_latitude,cre_time) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s','%s','%s','%s',%s,%s,now())"
sql+="ON DUPLICATE KEY UPDATE"
sql+=" city='%s',city_pinyin='%s',loc='%s',title='%s',img='%s',first_category='%s',second_category='%s',start_time=%s,end_time=%s,value=%s,price=%s,rebate=%s"
sql+=",s_name='%s',s_tel='%s',s_addr='%s',s_area='%s',s_longitude=%s,s_latitude=%s,upd_time=now()" 
def request_data(city):
    try:
        _log.info(api_url+city)
        response = urllib.request.urlopen(api_url+city)
        if response.status==200:
            result = response.read()
            return result.decode("utf8")
        else:
            _log.info(response.status+":"+response.msg+":"+response.reason)
    except Exception as err:
        _log.info("error:"+err.msg)
    return

def get_nodevalue(node,default_value=0):  
    if node.hasChildNodes():
        return node.childNodes[0].nodeValue if node else default_value  
    return default_value
  
def get_xmlnode(node,name):  
    return node.getElementsByTagName(name) if node else [] 

def parse_data(data,pinyin):
    #dom =xml.dom.minidom.parse('f:/nuomi.txt')
    dom = xml.dom.minidom.parseString(data)
    root = dom.documentElement
    
    url_nodes = get_xmlnode(root,'url')  
    insert=''
    try:
        with dbcon.cursor() as cursor:
            #cursor.execute("truncate table t_nuomi")
            for node in url_nodes:   
                locNode=node.getElementsByTagName("loc")[0]
                loc=locNode.childNodes[0].nodeValue
                
                dataNode = node.getElementsByTagName("data")[0]
                displayNode=dataNode.getElementsByTagName("display")[0]
                
                identifier=get_nodevalue(displayNode.getElementsByTagName("identifier")[0],'')
                city=get_nodevalue(displayNode.getElementsByTagName("city")[0],'').replace("'","\\'")
                title=get_nodevalue(displayNode.getElementsByTagName("title")[0],'').replace("'","\\'")
                image=get_nodevalue(displayNode.getElementsByTagName("image")[0],'')
                startTime=get_nodevalue(displayNode.getElementsByTagName("startTime")[0],0)
                endTime=get_nodevalue(displayNode.getElementsByTagName("endTime")[0],0)
                value=get_nodevalue(displayNode.getElementsByTagName("value")[0],0)
                price=get_nodevalue(displayNode.getElementsByTagName("price")[0],0)
                rebate=get_nodevalue(displayNode.getElementsByTagName("rebate")[0],0)
                
                firstCategory=get_nodevalue(displayNode.getElementsByTagName("firstCategory")[0],'').replace("'","\\'")
                secondCategory=get_nodevalue(displayNode.getElementsByTagName("secondCategory")[0],'').replace("'","\\'")

                #店铺信息
                shopsNode=displayNode.getElementsByTagName("shops")[0]
                shopNode=shopsNode.getElementsByTagName("shop")[0]
                s_name=get_nodevalue(shopNode.getElementsByTagName("name")[0],'').replace("'","\\'")
                s_tel=get_nodevalue(shopNode.getElementsByTagName("tel")[0],'').replace("'","\\'")
                s_addr=get_nodevalue(shopNode.getElementsByTagName("addr")[0],'').replace("'","\\'")
                s_area=get_nodevalue(shopNode.getElementsByTagName("area")[0],'').replace("'","\\'")
                s_longitude=get_nodevalue(shopNode.getElementsByTagName("longitude")[0],0)
                s_latitude=get_nodevalue(shopNode.getElementsByTagName("latitude")[0],0)

                insert=sql%(identifier,city,pinyin,loc,title,image,firstCategory,secondCategory,startTime,endTime,value,price,rebate,s_name,s_tel,s_addr,s_area,s_longitude,s_latitude
                            ,city,pinyin,loc,title,image,firstCategory,secondCategory,startTime,endTime,value,price,rebate,s_name,s_tel,s_addr,s_area,s_longitude,s_latitude)

                cursor.execute(insert)
                dbcon.commit()
    except IntegrityError  as err:
        _log.info("主键重复:",err)
        _log.info("错误SQL:",insert)
    except Exception as err:
        _log.info("error:",err)
        _log.info("错误SQL:",insert)
#     finally:
#         dbcon.close()

        
        
if __name__ == '__main__':
    _log.info("start--------------------------")
    in_city=input("全量同步请输入:ALL\n增量请输入开始城市的拼音!")
    in_city=in_city.strip().lower()
    arr=[]
    if in_city=='all':
        arr=citys
    else:
        if in_city in citys:
            arr=citys[citys.index(in_city):]
        else:
            _log.info("输入错误!")
            sys.exit(0)
    _log.info("将同步%d个城市,分别是:"%len(arr),arr)
    try:
        for city_pinyin in arr:
            city_pinyin=city_pinyin.strip()
            xmlstr=request_data(city_pinyin)
            parse_data(xmlstr,city_pinyin)
    except Exception as err:
        _log.info("error:",err)
    finally:
        dbcon.close()
    _log.info("end--------------------------")

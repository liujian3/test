# -*- coding: utf-8 -*-
"""
Created on Sat Jul 31 12:56:42 2021

@author: liujian
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 16:13:31 2021

@author: liujian
"""

import requests
import json
#from gevent import monkey; monkey.patch_socket()
#import gevent
import time
#import pandas as pd
rpth='/root/'
# #获得partners
# url='https://comtrade.un.org/Data/cache/partnerAreas.json'
# resp=requests.get(url)
# temp=json.loads(resp.content.decode('utf8'))
# partnersdict=temp['results']
# partners=sorted([y['id'] for y in partnersdict if y['id']!='all'])
# print('partners '+str(len(partners)))

# #获得reporters
# url='https://comtrade.un.org/Data/cache/reporterAreas.json'
# resp=requests.get(url)
# #reportersdict=resp.json(encoding='utf_8_sig')['results']
# temp=json.loads(resp.content[3:].decode('utf8'))
# reportersdict=temp['results']
# reporters=sorted([y['id'] for y in reportersdict if y['id']!='all'])
# print('reporters '+str(len(reporters)))

#分组
group=3
# partnersg=[','.join(partners[x:x+group]) for x in range(0,len(partners),group)]
# reportersg=[','.join(reporters[x:x+group]) for x in range(0,len(reporters),group)]
yms=['202105', '202104', '202103', '202102', '202101', '202012', '202011', '202010', '202009', '202008', '202007', '202006', '202005', '202004', '202003', '202002', 
     '202001', '201912', '201911', '201910', '201909', '201908', '201907', '201906', '201905', '201904', '201903', '201902', '201901', '201812', '201811', '201810', 
     '201809', '201808', '201807', '201806', '201805', '201804', '201803', '201802', '201801', '201712', '201711', '201710', '201709', '201708', '201707', '201706', 
     '201705', '201704', '201703', '201702', '201701', '201612', '201611', '201610', '201609', '201608', '201607', '201606', '201605', '201604', '201603', '201602', 
     '201601', '201512', '201511', '201510', '201509', '201508', '201507', '201506', '201505', '201504', '201503', '201502', '201501', '201412', '201411', '201410', 
     '201409', '201408', '201407', '201406', '201405', '201404', '201403', '201402', '201401', '201312', '201311', '201310', '201309', '201308', '201307', '201306', 
     '201305', '201304', '201303', '201302', '201301', '201212', '201211', '201210', '201209', '201208', '201207', '201206', '201205', '201204', '201203', '201202', 
     '201201', '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101', '201012', '201011', '201010', 
     '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001', '200912', '200911', '200910', '200909', '200908', '200907', '200906', 
     '200905', '200904', '200903', '200902', '200901', '200812', '200811', '200810', '200809', '200808', '200807', '200806', '200805', '200804', '200803', '200802', 
     '200801', '200712', '200711', '200710', '200709', '200708', '200707', '200706', '200705', '200704', '200703', '200702', '200701', '200612', '200611', '200610', 
     '200609', '200608', '200607', '200606', '200605', '200604', '200603', '200602', '200601', '200512', '200511', '200510', '200509', '200508', '200507', '200506', 
     '200505', '200504', '200503', '200502', '200501', '200412', '200411', '200410', '200409', '200408', '200407', '200406', '200405', '200404', '200403', '200402', 
     '200401', '200312', '200311', '200310', '200309', '200308', '200307', '200306', '200305', '200304', '200303', '200302', '200301', '200212', '200211', '200210', 
     '200209', '200208', '200207', '200206', '200205', '200204', '200203', '200202', '200201', '200112', '200111', '200110', '200109', '200108', '200107', '200106', 
     '200105', '200104', '200103', '200102', '200101', '200012', '200011', '200010', '200009', '200008', '200007', '200006', '200005', '200004', '200003', '200002', 
     '200001']
#yms=['2000']
#yms=sorted(pd.date_range('20000101','20210601',freq='M').astype('str').map(lambda x:x[:4]+x[5:7]).tolist(),reverse=True)
ymsg=[','.join(yms[x:x+group]) for x in  range(0,len(yms),group)]

partnersg=["0,577,336,585,652","56,304,882,64,626","276,568,624,678,776","842,534,660,90,581","710,796,174,728,184","826,218,162,10,647","251,530,28,841,836",
           "528,646,670,254,136","156,132,16,584,275","124,316,108,548,234","381,80,262,540,418","392,140,659,706,232","724,192,31,356,457","757,12,340,582,221",
           "643,748,807,200,890","699,498,222,74,239","784,729,879,800,637","792,732,334,388,531","36,520,583,100,459","410,764,461,472,532","208,752,280,636,492",
           "810,527,473,203,344","702,471,40,638,839","490,104,536,524,72","616,458,474,570,574","535,588,804,414,887","76,634,214,697,590","592,230,484,760,858",
           "360,58,854,328,772","212,818,426,795,496","300,466,720,24,690","372,654,480,838,8","612,188,780,658,682","620,698,266,499,368","579,376,332,717,835",
           "246,348,478,129,278","711,886,290,32,642","850,450,554,608,868","705,292,500,296,586","704,860,703,666,866","504,876,260,324,440","849,60,258,170,422",
           "899,442,148,226,238","837,716,178,364,891","580,312,152,688,84","566,798,352,175,196","191,768,166,204,398","591,308,44,144,86","788,598,533,462,428",
           "233,404,20,270,762","400,92,604,454,96","288,862,417,430,562","268,662,470,446,740","112,512,736,50","51,4,686,242,834","384,68,52,674,600",
           "48,694,408,434,558","231,320,516,120","70,116,508,894,180",]
def getdata(p,r,ym,freq):
    fpth=rpth+'uncomtrade/'+freq+p.replace(',','-')+'_'+r.replace(',','-')+'_'+ym.replace(',','-')+'.json'
    if os.path.exists(fpth) or os.path.exists(fpth.replace('uncomtrade','uncomtrade/'+ym)):
        return 0
    parmas={'p':p,'r':r,'max':10000,'freq':freq,'ps':ym,'px':'BEC'}
    pstr=urllib.parse.urlencode(parmas)

    url='http://comtrade.un.org/api/get?'+pstr

    print(p,r,ym,freq)
    print(url)
    #请求
    c=0
    temp=None
    while(c<5):
        
        try:
            resp=requests.get(url)
#            print(resp.text)
            if resp.text.replace('\x00','').startswith('USAGE LIMIT: Hourly'):
                print('USAGE LIMIT: Hourly')
                return 2
            temp=resp.json()
            if 'validation' not in temp:
                c+=1
                print(url+' 5 secs retry '+str(c))
                time.sleep(5)
                continue
            print(temp['validation']['status']['name'])
            if temp['validation']['count']['value']>10000:
                print('-'*20)
                raise Exception('-'*20)
                
            data=temp['dataset']
            print('return:'+str(len(data)))
            with open(fpth,'w',encoding='utf8') as f:
                json.dump(data,f)
            break
        except Exception as ex:
            with open(rpth+'error.log','a') as f:
                f.write(str(ex))
                f.write(url+'\n')
                if temp:
                    f.write(str(temp['validation']['count']['value']))
                    f.write('\n')
            # print(resp.json())
            c+=1
            print(url+' 5 secs retry'+str(c))
            time.sleep(5)
    if c>=5:
        with open(rpth+'error2.log','a') as f:
            f.write(url+'\n')
            if temp:
                f.write(str(temp['validation']['count']['value']))
                f.write('\n')
    return 1

import urllib
import os

import datetime

# res=[]

freq='M'
cnt=0
for ym in yms:    
    gs=[]
    
    for p in partnersg:
        for r in ['all']:
            #拼接参数
            
            print('*'*20+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            print(p,r,ym,freq)
            # time.sleep(2)
            # gs.append(gevent.spawn(getdata, p,r,ym,freq))
            x=getdata(p,r,ym,freq)
            if x==1:
                cnt+=1
            elif x==2:
#                cnt=90
#            if cnt>=90:
                cnt=0
                time.sleep(60*1)
            print(cnt)

            
    # gevent.joinall(gs)
    # time.sleep(60*60*0.5)

#df=pd.DataFrame(res)
#df.to_csv('d:/lj/data/test.csv')

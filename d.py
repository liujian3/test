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
import time
import pandas as pd
import os
if not os.path.exists('/root/uncomtrade'):
    os.mkdir('/root/uncomtrade')

#获得partners
url='https://comtrade.un.org/Data/cache/partnerAreas.json'
resp=requests.get(url)
temp=json.loads(resp.content.decode('utf8'))
partnersdict=temp['results']
partners=sorted([y['id'] for y in partnersdict if y['id']!='all'])
print('partners '+str(len(partners)))

#获得reporters
url='https://comtrade.un.org/Data/cache/reporterAreas.json'
resp=requests.get(url)
#reportersdict=resp.json(encoding='utf_8_sig')['results']
temp=json.loads(resp.content[3:].decode('utf8'))
reportersdict=temp['results']
reporters=sorted([y['id'] for y in reportersdict if y['id']!='all'])
print('reporters '+str(len(reporters)))

#分组
group=5
partnersg=[','.join(partners[x:x+group]) for x in range(0,len(partners),group)]
reportersg=[','.join(reporters[x:x+group]) for x in range(0,len(reporters),group)]
# yms=[str(x) for x in range(2020,1961,-1)]
yms=sorted(pd.date_range('19870101','20210101',freq='A').astype('str').map(lambda x:x[:4]).tolist())
ymsg=[','.join(yms[x:x+group]) for x in  range(0,len(yms),group)]

def getdata(p,r,ym,freq):
    fpth='/root/uncomtrade/'+freq+p.replace(',','-')+'_'+r.replace(',','-')+'_'+ym.replace(',','-')+'.json'
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
            with open('/root/error.log','a') as f:
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
        with open('/root/error2.log','a') as f:
            f.write(url+'\n')
            if temp:
                f.write(str(temp['validation']['count']['value']))
                f.write('\n')
    return 1

import urllib
import os

import datetime

# res=[]

freq='A'
cnt=0
for ym in ymsg:    
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
                cnt=90
            if cnt>=90:
                cnt=0
                time.sleep(60*60*0.5)
            print(cnt)

            
    # gevent.joinall(gs)
    # time.sleep(60*60*0.5)

#df=pd.DataFrame(res)
#df.to_csv('d:/lj/data/test.csv')

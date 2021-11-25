# -*- coding: utf-8 -*-
"""
Created on Wed May 12 13:54:36 2021

@author: Po
"""

from bs4 import BeautifulSoup
import requests
import json

url = r'https://gweb.wra.gov.tw/HydroInfo/?id=Index#'
def gwebFlow(url):
    resp = requests.get(url)
    
    if resp.status_code != 200:
        print('網址有問題')
        return
    soup = BeautifulSoup(resp.text, 'html.parser')


#%%
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
# import csv
# import xlwt
import time
import os
import random

os.chdir("C:\\Users\\A61M510\\Desktop\\gweb")
curdir = os.getcwd()

t = random.randint(3, 7)

#   讀取測站資訊，抓取日流量
stonInfor_df = pd.read_excel("資料測站資訊.xlsx", sheet_name="水位流量含砂量")
stonExit_df = stonInfor_df[stonInfor_df['存廢狀態'] == '現存']
stonNo, stonName, yearAll = stonExit_df['站號'].str.strip(), stonExit_df['站名'].str.strip(), stonExit_df['日流量']

# i = 0
for i in range(0, len(stonNo)):
    print(i)
    
    if pd.isnull(yearAll[i]):
        f = open("dayFlow\\noDAYFLOW_list.txt", 'a')
        f.write(stonName[i]+' ('+stonNo[i]+') '+ '沒有日流量的資料\n')
        f.close()
        print(stonName[i]+' ('+stonNo[i]+')'+ '沒有日流量的資料')
        continue
    else:
        # startTime = yearAll[i][:4]
        # endTime = yearAll[i][-4:]
        # yearInter = pd.date_range(start=startTime, end=str(int(endTime)+1), freq='Y')
        # yearInter = yearInter.format(formatter=(lambda x: x.strftime('%Y')))
        startYear = int(yearAll[i][:4])
        endYear = int(yearAll[i][-4:])
        os.makedirs("C:\\Users\\Po\\Desktop\\gweb\\dayFlow\\%s_%s" %(stonName[i], stonNo[i]))
        path = "C:\\Users\\Po\\Desktop\\gweb\\dayFlow\\%s_%s" %(stonName[i], stonNo[i])

        # year = startYear
        for year in range(startYear, endYear + 1):
            url = 'https://gweb.wra.gov.tw/HydroInfo/Chart/?containerID=chart-single-di&category=DI&sYear=%s&stno=%s++&timeType=dd&mode=0' %(year, stonNo[i])
            custom_headers = {'accept-encoding': 'gzip, deflate, br',
                              'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                              'referer': 'https://gweb.wra.gov.tw/Hydroinfo/?id=Index',
                              'x-requested-with': 'XMLHttpRequest'}
            
            resp = requests.get(url, headers = custom_headers)
            soup = BeautifulSoup(resp.text, 'html.parser')
            content = soup.find('input', id = 'chart-data')['value']
            contentDict = json.loads(content)
            
            
            flood_data = contentDict['series'][0]['data']
            yearRes = pd.DataFrame(flood_data, columns=['cms'])
            yearRes.to_csv(path+"\\" + "%s_dayFlow_%s.csv" % (stonNo[i], year), index=False, encoding='utf_8_sig')
            print("---------------已爬完%s_%s流量資料-----------------" %(stonName[i], year) )
            time.sleep(t)
        
        print("---------------%s, %s is DONE-----------------" %(i, stonName[i]) )

#%%

import os
import requests
from bs4 import BeautifulSoup
# from selenium import webdriver
import random
import time
import pandas as pd
import json
import calendar

os.chdir(r"C:\Users\Po\Desktop\gweb")

stonInfor_df = pd.read_excel("資料測站資訊.xlsx", sheet_name="水位流量含砂量")
stonExit_df = stonInfor_df[stonInfor_df['存廢狀態'] == '現存']
stonNo, stonName, yearAll = stonExit_df['站號'].str.strip(), stonExit_df['站名'].str.strip(), stonExit_df['時流量']

# i = 0
for i in range(0, len(stonNo)):
    print(str(i)+', '+stonNo[i]+'_'+stonName[i])
    
    if pd.isnull(yearAll[i]):
        f = open("hourFlow\\noHOURFLOW_list.txt", 'a')
        f.write(stonName[i]+' ('+stonNo[i]+') '+"沒有時流量資料\n")
        f.close()
        print(stonName[i]+' ('+stonNo[i]+') '+"沒有時流量資料\n")
        continue
    else:
        # startYear = int(yearAll[i][:4])
        # endYear = int(yearAll[i][-4:])
        
        
        gweb_url = 'https://gweb.wra.gov.tw/HydroInfo/history/QueryYearList'
        # gweb_resp = requests.get(gweb_url)
        # gweb_soup = BeautifulSoup(gweb_resp.text, "html.parser")
        formData = {'stno': ('%s' %stonNo[i]),
                    'category': 'DI',
                    'timeType': 'hh'}
        
        gweb_resp = requests.post(gweb_url, data=formData)
        gweb_soup = BeautifulSoup(gweb_resp.text, "html.parser")
        
        #   BS型別轉成str，再改成List
        yearList = json.loads(gweb_soup.encode("utf-8"))
        yearList.sort()
        
        
        for year in yearList:
            print(year)
            os.makedirs("C:\\Users\\Po\\Desktop\\gweb\\hourFlow\\%s_%s\\%s" %(stonName[i], stonNo[i], year))
            path = "C:\\Users\\Po\\Desktop\\gweb\\hourFlow\\%s_%s\\%s" %(stonName[i], stonNo[i], year)
            
            # mon = '12'
            for mon in range(1, 13):
                url = 'https://gweb.wra.gov.tw/HydroInfo/Chart/?containerID=chart-single-di&category=DI&sYear=%s&stno=%s++&timeType=hh&mode=0&sMonth=%s' %(year, stonNo[i], mon)
                custom_headers = {'accept': 'application/json, text/javascript, */*; q=0.01',
                                  'accept-encoding': 'gzip, deflate, br', 
                                  'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                                  'referer': 'https://gweb.wra.gov.tw/Hydroinfo/?id=Index'}
                resp = requests.get(url, headers = custom_headers)
                soup = BeautifulSoup(resp.text, "html.parser")
                content = soup.find('input', id='chart-data')['value']
                contentDict = json.loads(content)
                
                flood_data = contentDict['series'][0]['data']
                if not flood_data:
                    print('%s_%s/%s沒有流量資料' %(stonName[i], year, mon))
                    t = random.randint(3, 5)
                    time.sleep(t)
                    continue
                
                
                lastDay = calendar.monthrange(int(year), int(mon))[1]
                hour_range = pd.date_range(start=str(year)+'/'+str(mon)+'/'+'1', end=str(year)+'/'+str(mon)+'/'+str(lastDay), freq='H')      #len(flood_data)
                hour_range = hour_range.format(formatter=lambda x: x.strftime('%Y/%m/%d %H:%M'))
                
                if mon != 12:
                    hrRemain = pd.date_range(start=str(year)+'/'+str(mon)+'/'+str(lastDay), end=str(year)+'/'+str(mon+1)+'/'+'01', freq='H')
                    hrRemain = hrRemain.format(formatter=lambda x: x.strftime('%Y/%m/%d %H:%M'))
                    del hrRemain[0], hrRemain[-1]
                    for remain in hrRemain:
                        hour_range.append(remain)
                else:
                    hrRemain = pd.date_range(start=str(year)+'/'+str(mon)+'/'+str(lastDay), end=str(int(year)+1)+'/'+'01'+'/'+'01', freq='H')
                    hrRemain = hrRemain.format(formatter=lambda x: x.strftime('%Y/%m/%d %H:%M'))
                    del hrRemain[0], hrRemain[-1]
                    for remain in hrRemain:
                        hour_range.append(remain)
                        
                
                
                hourRes = pd.DataFrame(flood_data, index= hour_range, columns=['cms'])
                hourRes.to_csv(path+'\\' +"%s_hourFlow_%s_%s.csv" %(stonNo[i], year, mon), encoding='utf_8_sig')
                print("-----------------已爬完%s_%s/%s的日流量資料-----------------" %(stonName[i], year, mon))
                
                t = random.randint(3, 7)
                time.sleep(t)
            
        print("-----------------%s_%s is Done-----------------" %(stonName[i], year))
            
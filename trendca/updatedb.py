import requests
import json,io
import mysql.connector

import sched, time
from datetime import datetime

#today = datetime.now()
#print(time)

#d1 = today.strftime("%Y/%m/%d %H:%M:%S")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Waheguru@1987",
  database="optionchain"
)

mycursor = mydb.cursor()

def my_function():

    #new_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

    #headers = {'User-Agent': 'Mozilla/5.0'}
    #page = requests.get(new_url,headers=headers)
    
    baseurl = "https://www.nseindia.com/"
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                         'like Gecko) '
                         'Chrome/80.0.3987.149 Safari/537.36',
           'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
    session = requests.Session()
    request = session.get(baseurl, headers=headers, timeout=5)
    cookies = dict(request.cookies)
    try :
        response = session.get(url, headers=headers, timeout=5, cookies=cookies)
    except ConnectionError as e:
        print("Connection error")
    dajs = response.json()
    # with open('data.txt', 'w') as f:
    #    json.dump(dajs, f, ensure_ascii=False)
    expiry_dt = dajs['records']["expiryDates"][0]
    ce_values = [data['CE'] for data in dajs['records']['data'] if "CE" in data and data['expiryDate'] == expiry_dt]
    pe_values = [data['PE'] for data in dajs['records']['data'] if "PE" in data and data['expiryDate'] == expiry_dt]

    
    underlyingvalue = ce_values[0]['underlyingValue']
    variable = []
    today = datetime.now()
    d1 = today.strftime("%Y/%m/%d %H:%M:%S")
    for x in range(len(ce_values)):
        totalSellQuantity = ce_values[x]["totalSellQuantity"]
        totalBuyQuantity = ce_values[x]["totalBuyQuantity"]
        strikePrice = ce_values[x]["strikePrice"]
        askQty = ce_values[x]["askQty"]
        lastPrice = ce_values[x]["lastPrice"]
        bidQty = ce_values[x]["bidQty"]
        askPrice = ce_values[x]["askPrice"]
        pChange = ce_values[x]["pChange"]
        pchangeinOpenInterest = ce_values[x]["pchangeinOpenInterest"]
        underlying = ce_values[x]["underlying"]
        changeinOpenInterest = ce_values[x]["changeinOpenInterest"]
        totalTradedVolume = ce_values[x]["totalTradedVolume"]
        underlyingValue = ce_values[x]["underlyingValue"]
        impliedVolatility = ce_values[x]["impliedVolatility"]
        bidprice = ce_values[x]["bidprice"]
        openInterest = ce_values[x]["openInterest"]
        identifier = ce_values[x]["identifier"]
        changeful = ce_values[x]["change"]
        expiryDate = ce_values[x]["expiryDate"]

        uppervalue = underlyingValue+1000
        lowervalue = underlyingValue-1000
        #if strikePrice<uppervalue and strikePrice>lowervalue : 
        sql = "insert into option_master_ce (createddate,totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (d1,totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful)
        mycursor.execute(sql, val)
        mydb.commit()

    for y in range(len(pe_values)):
        totalSellQuantity = pe_values[y]["totalSellQuantity"]
        totalBuyQuantity = pe_values[y]["totalBuyQuantity"]
        strikePrice = pe_values[y]["strikePrice"]
        askQty = pe_values[y]["askQty"]
        lastPrice = pe_values[y]["lastPrice"]
        bidQty = pe_values[y]["bidQty"]
        askPrice = pe_values[y]["askPrice"]
        pChange = pe_values[y]["pChange"]
        pchangeinOpenInterest = pe_values[y]["pchangeinOpenInterest"]
        underlying = pe_values[y]["underlying"]
        changeinOpenInterest = pe_values[y]["changeinOpenInterest"]
        totalTradedVolume = pe_values[y]["totalTradedVolume"]
        underlyingValue = pe_values[y]["underlyingValue"]
        impliedVolatility = pe_values[y]["impliedVolatility"]
        bidprice = pe_values[y]["bidprice"]
        openInterest = pe_values[y]["openInterest"]
        identifier = pe_values[y]["identifier"]
        changeful = pe_values[y]["change"]
        expiryDate = pe_values[y]["expiryDate"]

        uppervalue = underlyingValue+1000
        lowervalue = underlyingValue-1000
        #if strikePrice<uppervalue and strikePrice>lowervalue : 
        sql = "insert into option_master_pe (createddate,totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (d1,totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful)
        mycursor.execute(sql, val)
        mydb.commit()
    

s = sched.scheduler(time.time, time.sleep)
def do_something(sc): 
    print("Doing stuff...")
    my_function()
    s.enter(60, 1, do_something, (sc,))

s.enter(60, 1, do_something, (s,))
s.run()
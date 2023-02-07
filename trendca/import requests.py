import requests
import json
import mysql.connector

import sched, time
from datetime import datetime

today = datetime.now()

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Waheguru@1987",
  database="optionchain"
)

mycursor = mydb.cursor()


def my_function():

    new_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(new_url,headers=headers)
    dajs = json.loads(page.text)

def fetch_oi(expiry_dt):
    ce_values = [data['CE'] for data in dajs['records']['data'] if "CE" in data and data['expiryDate'] == expiry_dt]
    pe_values = [data['PE'] for data in dajs['records']['data'] if "PE" in data and data['expiryDate'] == expiry_dt]
    d1 = today.strftime("%Y/%m/%d %H:%M:%S")
    
    underlyingvalue = ce_values[0]['underlyingValue']
    variable = []

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

        sql = "insert into option_master_ce (createddate, totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (d1, totalSellQuantity, totalBuyQuantity, strikePrice, askQty, lastPrice, bidQty, askPrice, pChange, pchangeinOpenInterest, expiryDate, underlying, changeinOpenInterest, totalTradedVolume, underlyingValue, impliedVolatility, bidprice, openInterest, identifier, changeful)
        mycursor.execute(sql, val)
        mydb.commit()
            
        

    def main():
        
        expiry_dt = '19-Jan-2023'
        fetch_oi(expiry_dt)
        
    def closest(lst, K): 
        
        return lst[min(range(len(lst)), key = lambda i: abs(lst[i]-K))] 

    if __name__ == '__main__':
        main()

s = sched.scheduler(time.time, time.sleep)
def do_something(sc): 
    print("Doing stuff...")
    my_function()
    s.enter(10, 1, do_something, (sc,))

s.enter(10, 1, do_something, (s,))
s.run()
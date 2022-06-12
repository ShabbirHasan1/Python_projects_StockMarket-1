import requests
import json

import time
import pandas as pd
import csv
from datetime import datetime
from datetime import timedelta





base_url = "https://kite.zerodha.com"
login_url = "https://kite.zerodha.com/api/login"
twofa_url = "https://kite.zerodha.com/api/twofa"
instruments_url = "https://api.kite.trade/instruments"
margins_url = "https://kite.zerodha.com/oms/user/margins"
positions_url = "https://kite.zerodha.com/oms/portfolio/positions"
holdings_url = "https://kite.zerodha.com/oms/portfolio/holdings"
orders_url = "https://kite.zerodha.com/oms/orders"
amo_orders_url = "https://kite.zerodha.com/oms/orders/amo"
regular_orders_url = "https://kite.zerodha.com/oms/orders/regular"



headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8"}
login_data = {"user_id":"xj1216",
              "password":"08676@Vf@123"}
nifty_50_symbols = {
    "COALINDIA":5215745,
    "CIPLA":177665,
    "SUNPHARMA":857857,
    "UPL":2889473,
    "NESTLEIND":4598529,
    "IOC":415745,
    "BAJAJ-AUTO":4267265,
    "SBILIFE":5582849,
    "TECHM":3465729,
    "DRREDDY":225537,
    "SBIN":779521,
    "WIPRO":969473,
    "DIVISLAB":2800641,
    "AXISBANK":1510401,
    "BRITANNIA":140033,
    "HINDALCO":348929,
    "SHREECEM":794369,
    "RELIANCE":738561,
    "HEROMOTOCO":345089,
    "HCLTECH":1850625,
    "HDFCLIFE":119553,
    "POWERGRID":3834113,
    "TATACONSUM":878593,
    "EICHERMOT":232961,
    "TCS":2953217,
    "NTPC":2977281,
    "INFY":408065,
    "ASIANPAINT":60417,
    "BPCL":134657,
    "GRASIM":315393,
    "MARUTI":2815745,
    "HINDUNILVR":356865,
    "ADANIPORTS":3861249,
    "INDUSINDBK":1346049,
    "ONGC":633601,
    "LT":2939649,
    "ITC":424961,
    "TITAN":897537,
    "HDFCBANK":341249,
    "BAJFINANCE":81153,
    "ICICIBANK":1270529,
    "KOTAKBANK":492033,
    "ULTRACEMCO":2952193,
    "BHARTIARTL":2714625,
    "TATASTEEL":895745,
    "JSWSTEEL":3001089,
    "HDFC":340481,
    "M&M":519937,
    "BAJAJFINSV":4268801,
    "TATAMOTORS":884737 }



from_date = "2015-01-01"
to_date = "2021-05-24"
timeframe = "minute"   #minute, 2minute, 3minute, 4minute, 5minute, 10minute, 15minute, 30minute, 30minute, hour, 2hour, 3hour,day


for items in nifty_50_symbols:
    try:

        symbol = items
        token = nifty_50_symbols[symbol]

        symbolName = symbol
        symbol_token = str(token)
        user_id = login_data["user_id"]

        filename =  str(symbolName) + "_" + str(timeframe) + str("___") + str(from_date) + str("___") + str(to_date) + ".csv"




        # LOGIN KITE ZERODHA ***************************************************************************
        while True:
            time.sleep(1)
            try:
                login = False
                with requests.Session() as kite:
                    url = login_url
                    #r = kite.get(url, headers=headers)
                    r = kite.post(url, data=login_data, headers=headers)
                    print(r.content)
                    data_json = json.loads(r.content)

                    if(data_json["status"]=="success"):
                        data_new = {"user_id":data_json["data"]["user_id"],"request_id":data_json["data"]["request_id"],"twofa_value":"086762"}
                        print(data_new)
                        rr = kite.post(twofa_url, data=data_new, headers=headers)
                        data_json_2fa = json.loads(rr.content)

                        print(data_json_2fa)

                        if(data_json_2fa["status"]=="success"):
                            login = True

                        print(rr.cookies["enctoken"])
                        enctoken = rr.cookies["enctoken"]
                if(login == True):
                    print("Login Sucess")
                    newheaders = {"authorization": "enctoken " + str(enctoken),
                              "sec-ch-ua-mobile":"?0",
                              "sec-fetch-dest":"empty",
                              "sec-fetch-mode" :"cors",
                              "sec-fetch-site":"same-origin",
                              "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
                              "x-kite-version":"2.7.0",
                              "x-kite-userid":login_data["user_id"]}
                    break
            except:
                time.sleep(1)
        # LOGIN KITE ZERODHA ***************************************************************************


        # ***************************************************************
        def every_50days_dict(start_date, end_date):


            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')



            dates_difference = []
            dates_difference.append(start.date())
            while 1:

                start = start + timedelta(days=60)
                next_days_re = start.date()
                after1 = next_days_re + timedelta(days=1)






                if (start >= end):
                    dates_difference.append(end.date())
                    dates_difference.append(end.date())
                    break
                else:
                    dates_difference.append(next_days_re)
                    dates_difference.append(after1)

            start_end_date_dict = []
            for i in range(len(dates_difference)):
                if(i+1 == len(dates_difference)):
                    break

                if(i %2 != 0):
                    continue

                start_date = dates_difference[i]
                end_date = dates_difference[i+1]

                #print(end_date)
                dict_entry = {"start_date":start_date, "end_date":end_date}
                start_end_date_dict.append(dict_entry)
            return start_end_date_dict
        # ***************************************************************


        def chart_data_to_CSV(filename, symbol_token, from_date, to_date):
            time.sleep(1)
            chart_url = "https://kite.zerodha.com/oms/instruments/historical/" + str(symbol_token) + "/" + str(timeframe) + "?user_id=" + str(
                user_id) + "&oi=1&from=" + str(from_date) + "&to=" + str(to_date)

            dataaa = kite.get(chart_url,headers=newheaders)
            return_data = json.loads(dataaa.content)
            print(return_data)
            if(return_data["status"] == "success"):
                with open(filename, 'a',newline='') as f:
                    write = csv.writer(f)
                    write.writerows(return_data["data"]["candles"])



                return True
            else:
                print("ERROR return DATA DATA")
                time.sleep(1000)
            return False






        datesss = every_50days_dict(from_date, to_date)
        for dates in datesss:
            start_date = dates["start_date"]
            end_date = dates["end_date"]

            chart_data_to_CSV(filename,symbol_token,start_date,end_date)

    except:
        print("Error Try Catuch")
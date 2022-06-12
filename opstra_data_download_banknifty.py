import datetime
import requests
import json
import time
import pandas as pd
import csv

#expirie_days = ["09DEC2021","02DEC2021","25NOV2021","18NOV2021","11NOV2021","03NOV2021","28OCT2021","21OCT2021","14OCT2021","07OCT2021","30SEP2021","23SEP2021","16SEP2021","09SEP2021","02SEP2021","26AUG2021","18AUG2021","12AUG2021","05AUG2021","29JUL2021","22JUL2021","15JUL2021","08JUL2021","01JUL2021","24JUN2021","17JUN2021","10JUN2021","03JUN2021","27MAY2021","20MAY2021","12MAY2021","06MAY2021","29APR2021","22APR2021","15APR2021","08APR2021","01APR2021","25MAR2021","18MAR2021","10MAR2021","04MAR2021","25FEB2021","18FEB2021","11FEB2021","04FEB2021","28JAN2021","21JAN2021","14JAN2021","07JAN2021","31DEC2020","24DEC2020","17DEC2020","10DEC2020","03DEC2020","26NOV2020","19NOV2020","12NOV2020","05NOV2020","29OCT2020","22OCT2020","15OCT2020","08OCT2020","01OCT2020","24SEP2020","17SEP2020","10SEP2020","03SEP2020","27AUG2020","20AUG2020","13AUG2020","06AUG2020","30JUL2020","23JUL2020","16JUL2020","09JUL2020","02JUL2020","25JUN2020","18JUN2020","11JUN2020","04JUN2020","28MAY2020","21MAY2020","14MAY2020","07MAY2020","30APR2020","23APR2020","16APR2020","09APR2020","01APR2020","26MAR2020","19MAR2020","12MAR2020","05MAR2020","27FEB2020","20FEB2020","13FEB2020","06FEB2020","30JAN2020","23JAN2020","16JAN2020","09JAN2020","02JAN2020","26DEC2019","19DEC2019","12DEC2019","05DEC2019","28NOV2019","21NOV2019","14NOV2019","07NOV2019","31OCT2019","24OCT2019","17OCT2019","10OCT2019","03OCT2019","26SEP2019","19SEP2019","12SEP2019","05SEP2019","29AUG2019","22AUG2019","14AUG2019","08AUG2019","01AUG2019","25JUL2019","18JUL2019","11JUL2019","04JUL2019","27JUN2019","20JUN2019","13JUN2019","06JUN2019","30MAY2019","23MAY2019","16MAY2019","09MAY2019","02MAY2019","25APR2019","18APR2019","11APR2019","04APR2019","28MAR2019","20MAR2019","14MAR2019","07MAR2019","28FEB2019","21FEB2019","14FEB2019","31JAN2019"]
#expirie_days = ["09DEC2021","02DEC2021","25NOV2021"]

#date = ["30DEC2021","23DEC2021","16DEC2021","09DEC2021","02DEC2021","25NOV2021","18NOV2021","11NOV2021","03NOV2021","28OCT2021","21OCT2021","14OCT2021","07OCT2021","30SEP2021","23SEP2021","16SEP2021","09SEP2021","02SEP2021","26AUG2021","18AUG2021","12AUG2021","05AUG2021","29JUL2021","22JUL2021","15JUL2021","08JUL2021","01JUL2021","24JUN2021","17JUN2021","10JUN2021","03JUN2021","27MAY2021","20MAY2021","12MAY2021","06MAY2021","29APR2021","22APR2021","15APR2021","08APR2021","01APR2021","25MAR2021","18MAR2021","10MAR2021","04MAR2021","25FEB2021","18FEB2021","11FEB2021","04FEB2021","28JAN2021","21JAN2021","14JAN2021","07JAN2021","31DEC2020","24DEC2020","17DEC2020","10DEC2020","03DEC2020","26NOV2020","19NOV2020","12NOV2020","05NOV2020","29OCT2020","22OCT2020","15OCT2020","08OCT2020","01OCT2020","24SEP2020","17SEP2020","10SEP2020","03SEP2020","27AUG2020","20AUG2020","13AUG2020","06AUG2020","30JUL2020","23JUL2020","16JUL2020","09JUL2020","02JUL2020","25JUN2020","18JUN2020","11JUN2020","04JUN2020","28MAY2020","21MAY2020","14MAY2020","07MAY2020","30APR2020","23APR2020","16APR2020","09APR2020","01APR2020","26MAR2020","19MAR2020","12MAR2020","05MAR2020","27FEB2020","20FEB2020","13FEB2020","06FEB2020","30JAN2020","23JAN2020","16JAN2020","09JAN2020","02JAN2020","26DEC2019","19DEC2019","12DEC2019","05DEC2019","28NOV2019","21NOV2019","14NOV2019","07NOV2019","31OCT2019","24OCT2019","17OCT2019","10OCT2019","03OCT2019","26SEP2019","19SEP2019","12SEP2019","05SEP2019","29AUG2019","22AUG2019","14AUG2019","08AUG2019","01AUG2019","25JUL2019","18JUL2019","11JUL2019","04JUL2019","27JUN2019","20JUN2019","13JUN2019","06JUN2019","30MAY2019","23MAY2019","16MAY2019","09MAY2019","02MAY2019","25APR2019","18APR2019","11APR2019","04APR2019","28MAR2019","20MAR2019","14MAR2019","07MAR2019","28FEB2019","21FEB2019","14FEB2019","31JAN2019"]

expirie_days = ["21APR2022","13APR2022","07APR2022","31MAR2022","24MAR2022","17MAR2022","10MAR2022","03MAR2022","24FEB2022"]
headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8"}
def trade_details_csv_store(dict_input, filename_in):
    while 1:
        try:
            exit_dict = dict_input
            # CSV WRITING ***********************************************************
            # in No CSV Header part..write..other wise leave------------
            def csv_header():
                try:
                    with open(filename_in) as f:
                        reader = csv.DictReader(f)
                        print(reader.fieldnames)
                        all_headers = reader.fieldnames
                    if "Symbol" in all_headers:
                        print("Yes")
                    else:
                        with open(filename_in, 'w', newline='') as f:
                            w = csv.writer(f)
                            w.writerow(exit_dict.keys())
                except:
                    with open(filename_in, 'w', newline='') as f:
                        w = csv.writer(f)
                        w.writerow(exit_dict.keys())
            csv_header()
            # in No CSV Header part..write..other wise leave------------
            with open(filename_in, 'a', newline='') as f:
                w = csv.writer(f)
                w.writerow(exit_dict.values())
            # CSV WRITING ***********************************************************
            break
        except:
            time.sleep(1)

symbol = "BANKNIFTY"

for exp_daye in expirie_days:
    print(exp_daye)
    date_exp = int(exp_daye[:2])
    print(date_exp)

    month_exp = exp_daye[2:5]
    print(month_exp)
    months_letters = {"JAN": 1,
                      "FEB": 2,
                      "MAR": 3,
                      "APR": 4,
                      "MAY": 5,
                      "JUN": 6,
                      "JUL": 7,
                      "AUG": 8,
                      "SEP": 9,
                      "OCT": 10,
                      "NOV": 11,
                      "DEC": 12,
                      }
    for itemss in months_letters:
        if (itemss == month_exp):
            month_number = months_letters[itemss]

    year_exp = int(exp_daye[5:10])

    today_date = datetime.date.today()
    current_time = datetime.datetime.now()
    expirydateee = current_time.replace(year=year_exp, month=month_number, day=date_exp)

    #print(str(expirydateee))

    minute_starting = 15
    hour_starting = 9
    trading_day = True

    while 1:
        time.sleep(1)
        minute_starting = minute_starting + 5
        if (minute_starting == 60):
            hour_starting = hour_starting + 1
            minute_starting = 0

        if (hour_starting == 15 and minute_starting == 35):
            break

        weekname = str(expirydateee.date().strftime("%A"))
        if (weekname == "Thursday"):
            start = 0
            end = 7
        if (weekname == "Wednesday"):
            start = 0
            end = 6
        if (weekname == "Tuesday"):
            start = 0
            end = 5


        for x in range(0, end):
            # print(x)
            time_delta = expirydateee - datetime.timedelta(days=x)
            #print(time_delta)

            testing_date = time_delta
            testing_expirie_day = exp_daye

            minute_starting = 15
            hour_starting = 9
            trading_day = True
            while 1:
                time.sleep(0.1)
                minute_starting = minute_starting + 5
                if (minute_starting == 60):
                    hour_starting = hour_starting + 1
                    minute_starting = 0

                if (hour_starting == 15 and minute_starting == 35):
                    break

                time_input = testing_date.replace(hour=hour_starting, minute=minute_starting, second=0, microsecond=0)
                timestamp = time_input.timestamp()
                final_time_stamp = int(round(timestamp))
                print(time_input)

                file_nameee = str(testing_date) + ".csv"

                try:
                    # url_is = "https://opstra.definedge.com/api/optionsimulator/optionchain/1636516200&NIFTY&11NOV2021"
                    url_is = "https://opstra.definedge.com/api/optionsimulator/optionchain/" + str(
                        final_time_stamp) + "&BANKNIFTY&" + str(testing_expirie_day)
                    r = requests.get(url_is, headers=headers)
                    data_json = json.loads(r.content)
                    print(data_json)
                    trading_day = True
                except:
                    trading_day = False

                if (trading_day == False):
                    break

                file_namee = str(testing_date.date()) + ".csv"

                input_dict = {"DateAndTime": time_input, "TimeStamp": final_time_stamp, "Symbol": symbol,
                              "Expirie": exp_daye, "options_chain": data_json}
                trade_details_csv_store(input_dict, file_namee)

        break

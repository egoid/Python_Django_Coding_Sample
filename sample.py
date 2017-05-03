## Python/Django Coding Samples
##
## This is a Cron Task which creates a two datastores made from two different data sets from Quandl - and updating every 15 minutes if there are changes.
##
## By Adam T. 2016
##

from time import gmtime, strftime
from django.conf.urls import patterns, url, include
from django.conf import settings
from django.conf.urls import patterns, url
from django.db import connection
from portal import views
from yahoo_finance import Share
import after_response
import datetime
import logging
import pandas as pd
import quandl
import requests
import time
logging.basicConfig()


BuildSF1Database()
Update_DB_Dispatcher.after_response()

BuildStockDatabase()
Update_Stocks_Dispatcher.after_response()

def BuildStockDatabase():
    cursor = connection.cursor();
    cursor.execute("select distinct ticker from portal_stock")
    for item in dictfetchall(cursor):
        try:
            cursor.execute("DROP TABLE stock_" + str(item['ticker']) + "")
        except:
            print("error - table doesn't exist")
        cursor.execute("CREATE TABLE IF NOT EXISTS stock_" + str(item['ticker']) + " ("
                        "`id`INTEGER(2) UNSIGNED AUTO_INCREMENT,"
                        "`last_date` DATETIME,"
                        "`Adj_Open` VARCHAR(255),"
                        "`Adj_High` VARCHAR(255),"
                        "`Adj_Low` VARCHAR(255),"
                        "`Adj_Close` VARCHAR(255),"
                        "`Adj_Volume` VARCHAR(255),"
                        "PRIMARY KEY (id) );")
        try:
            stock = quandl.get(["EOD/" + str(item['ticker']) ])
            for index in stock.index.tolist():
                date_index = pd.to_datetime(index)
                adj_open = stock.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Open"]
                adj_high = stock.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_High"]
                adj_low = stock.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Low"]
                adj_close = stock.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Close"]
                adj_volume = str(stock.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Volume"])
                cursor.execute("INSERT INTO stock_" + str(item['ticker']) + " (last_date, Adj_Open, Adj_High, Adj_Low, Adj_Close, Adj_Volume) VALUES"
                            " ('" + str(date_index) + "','" + str(adj_open) + "','" + str(adj_high) + "','" +str(adj_low) + "','" +str(adj_close) + "','" +str(1.0) + "');")
        except:
            print("error - fetching Quandl Data")

@after_response.enable
def Update_DB_Dispatcher():
    RefreshDB()
    for i in xrange(0,365):
        print("Your Database Dispatcher is now turned on!")
        t = datetime.datetime.today()
        future = datetime.datetime(t.year,t.month,t.day,2,0)
        if t.hour >= 2:
            future += datetime.timedelta(days=1)
        print("Going to sleep until " + str(future-t) + ", and then I will refresh your database!")
        time.sleep((future-t).seconds)
        RefreshDB()

@after_response.enable
def RefreshStockPrices():
    cursor = connection.cursor();
    cursor.execute("select distinct ticker from portal_stock")
    for table_stock in dictfetchall(cursor):
        try:
            cursor.execute("select * from daily_" + str(table_stock['ticker']))
        except:
            cursor.execute("CREATE TABLE IF NOT EXISTS daily_" + str(table_stock['ticker']) + " ("
                            "`id`INTEGER(1) DEFAULT 1,"
                            "`value` VARCHAR(12),"
                            "PRIMARY KEY (id) );")
            cursor.execute("INSERT into daily_" + str(table_stock['ticker']) + " (value) VALUES ('0') ")
        try:
            stock_share = Share(str(table_stock['ticker']))
            stock_value = stock_share.get_open()
            update_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            cursor.execute("UPDATE daily_" + str(table_stock['ticker']) + " SET value = " + str(stock_value) + " WHERE id = 1")
            cursor.execute("UPDATE portal_stock SET current_price = " + str(stock_value) + " WHERE ticker = '" + str(table_stock['ticker']) + "'")
            cursor.execute("UPDATE portal_stock SET update_date = " + str(update_time) + "' WHERE ticker = '" + str(table_stock['ticker']) + "'")
            print(str(table_stock['ticker'])+ " updated to : " + str(stock_value))
        except:
            print("Error updating" + str(table_stock['ticker']))

@after_response.enable
def Update_Stocks_Dispatcher():
    print("Your Stock Price Dispatcher is now turned on!")
    RefreshStockPrices()
    print("Going to sleep for an 15 minutes, and then I will refresh your database!")
    for i in xrange(0,365):
        time.sleep(360)
        RefreshStockPrices()

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

def RefreshDB():
    cursor = connection.cursor();
    cursor.execute("select distinct ticker from portal_stock")
    for item in dictfetchall(cursor):
        atime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        stock_ticker = item['ticker']
        cursor.execute("select last_date from stock_"+str(stock_ticker)+" order by last_date DESC LIMIT 1")
        for last_date in dictfetchall(cursor):
            if time.strptime(atime, "%Y-%m-%d %H:%M:%S") > time.strptime(str(last_date['last_date']), "%Y-%m-%d %H:%M:%S"):
                starttime = str(last_date['last_date'])[0:10]
                endtime = strftime("%Y-%m-%d", gmtime())

                stock = quandl.get(["EOD/" + str(stock_ticker)], start_date=endttime, end_date=endtime)
                for index in stock.index.tolist():
                    date_index = pd.to_datetime(index)
                    adj_open = a.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Open"]
                    adj_high = a.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_High"]
                    adj_low = a.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Low"]
                    adj_close = a.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Close"]
                    adj_volume = str(a.loc[date_index]['EOD/' + str(item['ticker']) + " - Adj_Volume"])
                    cursor.execute("INSERT INTO stock_" + str(item['ticker']) + " (last_date, Adj_Open, Adj_High, Adj_Low, Adj_Close, Adj_Volume) VALUES"
                                " ('" + str(date_index) + "','" + str(adj_open) + "','" + str(adj_high) + "','" +str(adj_low) + "','" +str(adj_close) + "','" +str(1.0) + "');")




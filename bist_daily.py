import yfinance as yf
yf.pdr_override()
from pandas_datareader import data as pr
from bs4 import BeautifulSoup as bs
from bs4.element import Comment
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
from dateutil.relativedelta import relativedelta
import time


def g_stocks(stock, start, end):
    """Google finance.
    Returns historical data for given stocks for given time interval using Google Finance via using google sheets.
    Function has 10 sec time sleep, it may change, due to api limits.
    """
    try:
        scope =  ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open('stocks').sheet1
        sheet.update_cell(1, 7, stock)
        sheet.update_cell(1, 9, str(start))
        sheet.update_cell(1, 10, str(end))
        if not sheet.get_all_records() == []:
            df = pd.DataFrame(sheet.get_all_records())[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.drop(df[df['Close'].astype(str).str.len() == 0].index, inplace = True)
            type_map = {'Open': float,
                        'Close': float,
                        'High': float,
                        'Low': float,
                        'Volume':int}   
            df = df.astype(type_map)
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            print(f'{stock} DONE BY GOOGLE FINANCE')
            return df.set_index('Date')
        else:
            print(f'{stock} DONE BY GOOGLE FINANCE')
            return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).set_index('Date')
    except gspread.exceptions.APIError:
        print('Error 429: Too Many Requests')
        print('10sec sleep')
        time.sleep(10)
        return get_hist_data(stock = stock, start_date=start, end_date=end)

def get_hist_data(stock, start_date, end_date):
    """"Historical Data Function.
    Returns historical data for given symbol for given time interval. Function uses Yahoo finance, 
    but in case of missing data, function uses Google finance.
    """
    df = pr.DataReader(stock + '.IS', start=start_date, end=end_date)
    df.drop(columns=['Adj Close'], inplace=True)
    if df.shape[0] <= 1:
        df = g_stocks(stock, start_date, end_date)
    return df


def Delta_Time(years = 0, months = 0, weeks = 0, days = 0, start = dt.datetime.today()):
    """ Delta time function.
    Creates start and end dates. Years, months, weeks, days are set by default to 0, and start date is set to today.
    """

    start_date = start - relativedelta(years = years, months = months, weeks = weeks, days = days)
    end_date = dt.datetime.today()
    return start_date.date(), end_date.date()
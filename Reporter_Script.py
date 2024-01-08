import requests
import yfinance as yf
import pandas as pd
import datetime
from datetime import date
import numpy as np
import os  
from alive_progress import alive_bar
import sys

file_directory=r"C:\Users\Admin\Desktop\New folder"

def Reporter():  
    data_url="https://raw.githubusercontent.com/danhuynh23/kraftwerk_testing/main/nasdaq_screener_1703921359265.csv"
    Nasdaq=pd.read_csv(data_url)
    Nasdaq=Nasdaq.astype(str)
    tickers=(Nasdaq['Symbol'].tolist())

    file_directory=r"C:\Users\Admin\Desktop\New folder"

    api_key = '1822bea789msh219ec7e462e75a0p1d9a2cjsn7596777f7daf'
    base_url = 'https://www.alphavantage.co/query'

    req_ffr = requests.get(
        base_url,
        params={
            "function": "FEDERAL_FUNDS_RATE",
            "apikey": api_key,
            "interval":"daily",
            "outputsize":"full"

        }
    )
    data_ffr=req_ffr.json()
    FED_FUND_RATE=pd.DataFrame(data_ffr['data'])
    FED_FUND_RATE.index=pd.to_datetime(FED_FUND_RATE['date'],format='%Y-%m-%d')
    FED_FUND_RATE.drop(columns=['date'],inplace=True)
    FED_FUND_RATE.rename(columns={"value":"Rf"},inplace=True)
    FED_FUND_RATE.to_csv(file_directory+"\FED_RATE.csv") 


    end = date.today()
    start= end - datetime.timedelta(weeks=12)
    print("Loading Data, this will take a while")
    data = yf.download(tickers, start=start, end=end)
    Adj_Close=data['Adj Close'].dropna(axis=1)
    Adj_Close.index =  pd.to_datetime(Adj_Close.index, format='%Y-%m-%d')
    Returns_df=Adj_Close.pct_change()
    data.to_csv(file_directory+"\data.csv")
        
    ticker_objects_dict={}
    with alive_bar(len(tickers)) as bar:
        for i in tickers: 
            object=yf.Ticker(i)
            ticker_objects_dict.update({i:object}) 
            bar()
    
    print("Initializing Ticker Objects")
    with alive_bar(len(ticker_objects_dict)) as bar:
        for tickers_object in ticker_objects_dict:
            ticker_objects_dict[tickers_object].history_metadata
            bar()

    Adj_Close.index =  pd.to_datetime(Adj_Close.index, format='%Y-%m-%d')
    Returns_df=Adj_Close.pct_change()
    Returns_df.dropna(axis=0,inplace=True)
    Dict_of_returns={}

    for i in Returns_df.index: 
        Dict_of_returns.update({i:Returns_df.loc[i]})
    Returns_df.index=Returns_df.index.astype(str)
    Returns_daily_df=Returns_df.T
    Returns_daily_df.to_csv(file_directory+"\Daily Returns.csv")

    balance_sheets={}
    income_stmt_sheets={}

    print("Getting balance sheets")
    with alive_bar(len(ticker_objects_dict)) as bar: 
        for tickers_object in ticker_objects_dict:
            balance_sheets.update({tickers_object:(ticker_objects_dict[tickers_object].balance_sheet)})
            income_stmt_sheets.update({tickers_object:(ticker_objects_dict[tickers_object].get_income_stmt())})
            bar()
        
    Dict_of_book_values={}
    Dict_of_captialization={}
    Dict_of_asset={}
    for company in balance_sheets:
        try:
            Dict_of_book_values.update({company:balance_sheets[company].loc['Common Stock Equity'][0]})
            Dict_of_captialization.update({company:balance_sheets[company].loc['Total Capitalization'][0]})
            Dict_of_asset.update({company:(balance_sheets[company].loc['Total Assets'][0]/balance_sheets[company].loc['Total Assets'][1])})
        except: 
            pass 
        
    Dict_of_Operating_Income={}
    for company in income_stmt_sheets:
        try:
            Dict_of_Operating_Income.update({company:income_stmt_sheets[company].loc['OperatingIncome'][0]})
        except:
            pass
    
    Capitalization_and_book_value_df=pd.DataFrame({'Capitalization':pd.Series(Dict_of_captialization),'Book Value':pd.Series(Dict_of_book_values),'Operating Income':pd.Series(Dict_of_Operating_Income),"Investment":pd.Series(Dict_of_asset)})
    Capitalization_and_book_value_df.to_csv(file_directory+"\Company Data.csv")
    Returns_df.to_csv(file_directory+"Return_Data.csv")
    return(Capitalization_and_book_value_df,Returns_df)

def main():    
    print(Reporter())
    print("Ran")


if __name__=='__main__':
    main()

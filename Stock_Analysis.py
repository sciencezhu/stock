import numpy as np
from scipy.stats import gamma
import matplotlib.pyplot as plt
import pandas as pd
import sys, time, re, csv, glob, os, operator, cmath, datetime
# Package and modules for importing data; this code may change depending on pandas version
import pandas_datareader.data as web   
import Stock_DB_Class_Self as StockDB
from dateutil.relativedelta import *
import sqlite3 as lite
 



def main():
    # We will look at stock prices over the past year, starting at January 1, 2016
    
    
#    print (lite.version)
    db = StockDB.FinanceDataStore("db.db")
    
#    print (db.TableNotExist())
   
    db.setup_db_for_hist_prices_storage(["AAPL"], start= datetime.date.today()-relativedelta(years=2), end = datetime.date.today()-relativedelta(months=1))
    
    db.setup_db_for_hist_prices_storage(["AAPL"], start= datetime.date.today()-relativedelta(months=2), end = datetime.date.today())
    
#    db.scan_and_input_recent_prices(["AAPL"], num_days_for_updates = 100 )
    
#    print (db.retrieve_stocklist_fr_db())
    
#    print (db.TableEmpty())
    
    db.close_db()
    
    
if __name__ == '__main__':
    main()
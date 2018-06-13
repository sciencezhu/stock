import re, sys, os, time, datetime, csv
from dateutil.relativedelta import *
import pandas
import sqlite3 as lite
#from yahoo_finance_historical_data_extract import YFHistDataExtr
#from Yahoo_finance_YQL_company_data import YComDataExtr #use for fast retrieval of data.
import pandas_datareader.data as web   # Package and modules for importing data; this code may change depending on pandas version

 
class FinanceDataStore(object):
    """ For storing and retrieving stocks data from database.
 
    """
    def __init__(self, db_full_path):
        """ Set the link to the database that store the information.
            Args:
                db_full_path (str): full path of the database that store all the stocks information.
 
        """
        self.con = lite.connect(db_full_path)
        self.cur = self.con.cursor()
        self.hist_data_tablename = 'histprice' #differnt table store in database
#        self.divdnt_data_tablename = 'dividend'
 
        ## set the date limit of extracting.(for hist price data only)
        self.set_data_limit_datekey = '' #set the datekey so 
 
        ## output data
        self.hist_price_df = pandas.DataFrame()
#        self.hist_div_df = pandas.DataFrame()
        
#        self.hist_price_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
#                                        schema=None, if_exists='append', index=True,
#                                        index_label=None, chunksize=None, dtype=None)
        

    def close_db(self):
        """ For closing the database. Apply to self.con
        """
        self.con.close()

    def retrieve_stocklist_fr_db(self):
        """ Retrieve the stocklist from db
            Returns:
                (list): list of stock symbols.
        """
        command_str = "SELECT DISTINCT SYMBOL FROM %s "% self.hist_data_tablename
        self.cur.execute(command_str)
        rows = self.cur.fetchall()
 
#        self.close_db()
#        return [n[0].encode() for n in rows]
        return [n[0] for n in rows]
    
    def TableEmpty(self):
        command_str ="select * from %s "% self.hist_data_tablename
        self.cur.execute(command_str)
        table_exist = self.cur.fetchone()
        if table_exist:
            return 0
        else:
            return 1        
    
    def TableNotExist(self):
        command_str = 'SELECT name FROM sqlite_master WHERE type="table" AND name=\"'+self.hist_data_tablename+"\""
        if (self.cur.execute(command_str).fetchone()==None):
            return 1
        else:
            return 0
        
    
    def break_list_to_sub_list(self,full_list, chunk_size = 45):
        """ Break list into smaller equal chunks specified by chunk_size.
            Args:
                full_list (list): full list of items.
            Kwargs:
                chunk_size (int): length of each chunk.
            Return
                (list): list of list.
        """
        if chunk_size < 1:
            chunk_size = 1
        return [full_list[i:i + chunk_size] for i in range(0, len(full_list), chunk_size)]
 
    def setup_db_for_hist_prices_storage(self, stock_sym_list, start= datetime.date.today()-relativedelta(years=20), end = datetime.date.today()):
        """ Get the price and dividend history and store them to the database for the specified stock sym list.
            The length of time depends on the date_interval specified.
            Connection to database is assuemd to be set.
            For one time large dataset (where the hist data is very large)
            Args:
                stock_sym_list (list): list of stock symbol.
 
        """
        for sub_list in self.break_list_to_sub_list(stock_sym_list):
#            print ('processing sub list', sub_list) 
#            need to check duplicates in the database            
            for susublist in sub_list:
#                print (susublist)
#                print(type(susublist))
                stock_df = web.DataReader(susublist, "yahoo", start, end)
                stock_df['SYMBOL'] = susublist
                stock_df.reset_index(inplace=True,drop=False)
                print(stock_df.head())
                                
                if (self.TableNotExist()):
                    print ("table not exist")
                    stock_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
                                        schema=None, if_exists='append', index=True,
                                        index_label=None, chunksize=None, dtype=None)
                else:
                    if(self.TableEmpty()):
                        print ("table is empty")
                        stock_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
                                        schema=None, if_exists='append', index=True,
                                        index_label=None, chunksize=None, dtype=None)
                    else:
                        if (susublist not in self.retrieve_stocklist_fr_db()):
                            print ("stock not in list")
                            print (susublist)
                            print (self.retrieve_stocklist_fr_db())
                            stock_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
                                        schema=None, if_exists='append', index=True,
                                        index_label=None, chunksize=None, dtype=None)
                        else:
                            print ("stock in list")
                            self.DfCompareAndMerge(stock_df)
                    
#        self.close_db()
 
    def DfCompareAndMerge(self, stock_df):
#        print (stock_df.head())
#        print (stock_df.columns.values.tolist())
        DF_Date_Min= stock_df['Date'].min()
        DF_Date_Max= stock_df['Date'].max()
        stock_name = stock_df['SYMBOL'].tolist()[0]
        print (stock_name)
        
        #retrieve stock date only and then compare with the above max and min dates and then choose what to do.
        self.cur.execute('SELECT Max(Date) FROM '+self.hist_data_tablename+' WHERE SYMBOL=?', (stock_name,) )
        db_max = self.cur.fetchone()[0]
        self.cur.execute('SELECT Min(Date) FROM '+self.hist_data_tablename+' WHERE SYMBOL=?', (stock_name,) )
        db_min = self.cur.fetchone()[0]
        
        
#        print (DF_Date_Min)
#        print (DF_Date_Max)
#        DB_Date_Min = 
        
        
        
        
        
        
        return 0    
        
        
#        df1[~df1.isin(df2)].dropna()
    
#    def Retrieve_Max_Min_Date(self, stock_symbol):
        
    
    
    
    
    def scan_and_input_recent_prices(self, stock_sym_list, num_days_for_updates = 30 ):
        """ Another method to input the data to database. For shorter duration of the dates.
            Function for storing the recent prices and set it to the databse.
            Use with the YQL modules.
            Args:
                stock_sym_list (list): stock symbol list.
            Kwargs:
                num_days_for_updates: number of days to update. Cannot be set too large a date.
                                    Default 10 days.
 
        """
        
        self.setup_db_for_hist_prices_storage(stock_sym_list, start= datetime.date.today()-relativedelta(days=num_days_for_updates), end = datetime.date.today())
        
#        w = YComDataExtr()
#        w.set_full_stocklist_to_retrieve(stock_sym_list)
#        w.set_hist_data_num_day_fr_current(num_days_for_updates)
#        w.get_all_hist_data()
 
        ## save to one particular funciton
        #save to sql -- hist table
#        w.datatype_com_data_allstock_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
#                                schema=None, if_exists='append', index=True,
#                                index_label=None, chunksize=None, dtype=None)
 
 
    def retrieve_hist_data_fr_db(self, stock_list=[], select_all =1):
        """ Retrieved a list of stocks covering the target date range for the hist data compute.
            Need convert the list to list of str
            Will cover both dividend and hist stock price
            Kwargs:
                stock_list (list): list of stock symbol (with .SI for singapore stocks) to be inputted.
                                    Will not be used if select_all is true.
                select_all (bool): Default to turn on. Will pull all the stock symbol
 
        """
        stock_sym_str = ''.join(['"' + n +'",' for n in stock_list])
        stock_sym_str = stock_sym_str[:-1]
        #need to get the header
        command_str = "SELECT * FROM %s where symbol in (%s)"%(self.hist_data_tablename,stock_sym_str)
        if select_all: command_str = "SELECT * FROM %s "%self.hist_data_tablename
        self.cur.execute(command_str)
        headers =  [n[0] for n in self.cur.description]
 
        rows = self.cur.fetchall() # return list of tuples
        self.hist_price_df = pandas.DataFrame(rows, columns = headers) #need to get the header?? how to get full data from SQL
 
        ## dividend data extract
#        command_str = "SELECT * FROM %s where symbol in (%s)"%(self.divdnt_data_tablename,stock_sym_str)
#        if select_all: command_str = "SELECT * FROM %s "%self.divdnt_data_tablename
# 
#        self.cur.execute(command_str)
#        headers =  [n[0] for n in self.cur.description]
# 
#        rows = self.cur.fetchall() # return list of tuples
#        self.hist_div_df = pandas.DataFrame(rows, columns = headers) #need to get the header?? how to get full data from SQL
# 
#        self.close_db()
 
    def add_datekey_to_hist_price_df(self):
        """ Add datekey in the form of yyyymmdd for easy comparison.
 
        """
        self.hist_price_df['Datekey'] = self.hist_price_df['Date'].map(lambda x: int(x.replace('-','') ))
 
    def extr_hist_price_by_date(self, date_interval):
        """ Limit the hist_price_df by the date interval.
            Use the datekey as comparison.
            Set to the self.hist_price_df
 
        """
        self.add_datekey_to_hist_price_df()
        target_datekey = self.convert_date_to_datekey(date_interval)
        self.hist_price_df = self.hist_price_df[self.hist_price_df['Datekey']>=target_datekey]
 
    def convert_date_to_datekey(self, offset_to_current = 0):
        """ Function mainly for the hist data where it is required to specify a date range.
            Default return current date. (offset_to_current = 0)
            Kwargs:
                offset_to_current (int): in num of days. default to zero which mean get currnet date
            Returns:
                (int): yyymmdd format
 
        """
        last_eff_date_list = list((datetime.date.today() - datetime.timedelta(offset_to_current)).timetuple()[0:3])
 
        if len(str(last_eff_date_list[1])) == 1:
            last_eff_date_list[1] = '0' + str(last_eff_date_list[1])
 
        if len(str(last_eff_date_list[2])) == 1:
            last_eff_date_list[2] = '0' + str(last_eff_date_list[2])
 
        return int(str(last_eff_date_list[0]) + last_eff_date_list[1] + str(last_eff_date_list[2]))

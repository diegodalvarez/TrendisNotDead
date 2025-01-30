# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 09:15:01 2025

@author: Diego
"""

import os
import pandas as pd


class DataCollect: 
    
    def __init__(self):
        
        self.dir       = os.path.dirname(os.path.abspath(__file__))  
        self.root_path = os.path.abspath(
            os.path.join(os.path.abspath(
                os.path.join(self.dir, os.pardir)), os.pardir))
        
        self.data_path = os.path.join(self.root_path, "data")
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
            
        self.bad_tickers = ["BTC", "UX", "SER", "SFR", "UX"]
        
        self.bbg_path   = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\root\fut_tickers.xlsx"
        self.df_tickers = (pd.read_excel(
            io = self.bbg_path).
            query("contract != @self.bad_tickers"))

        self.vol_px_rtn = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\AdjustedVolTargetedPXFront\ConstantVolTargeting"

    def get_vol_futures(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.data_path, "PX.parquet")
        try:
            
            if verbose == True: print("Trying to find raw PX data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")  
        
        
            tickers = self.df_tickers.contract.drop_duplicates().sort_values().to_list()
            paths = [
                os.path.join(self.vol_px_rtn, path + ".parquet")
                for path in tickers]

            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow"))
            
            df_out.to_parquet(path = file_path, engine = "pyarrow")

        return df_out
    
if __name__ == "__main__": DataCollect().get_vol_futures(verbose = True)
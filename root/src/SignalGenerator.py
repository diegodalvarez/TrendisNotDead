# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 09:26:54 2025

@author: Diego
"""

import os
import numpy as np
import pandas as pd
from   DataCollect import DataCollect
from   tqdm import tqdm
tqdm.pandas()

class SignalGenerator(DataCollect):
    
    def __init__(self) -> None:
        
        super().__init__()
        self.windows = {
            "short_window": 10,
            "long_window" : 100}
        
    def _get_signal(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.assign(
            short_mean = lambda x: x.px.ewm(span = self.windows["short_window"], adjust = False).mean(),
            long_mean  = lambda x: x.px.ewm(span = self.windows["long_window"], adjust = False).mean(),
            sig_adjust = lambda x: x.px.diff().ewm(span = 10, adjust = False).std(),
            signal     = lambda x: (x.short_mean - x.long_mean) / x.sig_adjust,
            lag_signal = lambda x: x.signal.shift()).
            dropna())
        
        return df_out
        
    def get_signal(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.data_path, "Signal.parquet")
        try:
            
            if verbose == True: print("Trying to find signal data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")  
        
            df_out = (self.get_vol_futures().groupby(
                "security").
                apply(self._get_signal).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def get_signal_rtn(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.data_path, "SignalRtn.parquet")
        try:
            
            if verbose == True: print("Trying to find signal rtn data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")  
        
            df_out = (self.get_signal()[
                ["date", "security", "signal", "lag_signal", "px_rtn", "vol_rtn"]].
                melt(id_vars = ["date", "security", "signal", "lag_signal"]).
                assign(
                    perf_rtn = lambda x: np.sign(x.signal) * x.value,
                    lag_rtn  = lambda x: np.sign(x.lag_signal) * x.value))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def _get_cum_rtn(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.set_index(
            "date")
            [["perf_rtn", "lag_rtn"]].
            apply(lambda x: np.cumprod(1 + x) - 1))
        
        return df_out
    
    def get_cum_rtn(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.data_path, "AlphaDecay.parquet")
        try:
            
            if verbose == True: print("Trying to find Alpha Decay")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")  
            
            df_out = (self.get_signal_rtn().assign(
                group_var = lambda x: x.security + "+" + x.variable).
                groupby("group_var").
                progress_apply(lambda group: self._get_cum_rtn(group)).
                reset_index().
                assign(
                    security  = lambda x: x.group_var.str.split("+").str[0].str.split(" ").str[0].str.replace("1", ""),
                    rtn_group = lambda x: x.group_var.str.split("+").str[-1]).
                drop(columns = ["group_var"]).
                assign(alpha_decay = lambda x: x.lag_rtn - x.perf_rtn))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
        
def main():    

    SignalGenerator().get_signal(verbose = True)
    SignalGenerator().get_signal_rtn(verbose = True)
    SignalGenerator().get_cum_rtn(verbose = True)
    
if __name__ == "__main__": main()
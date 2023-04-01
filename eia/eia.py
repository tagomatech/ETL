# """ eia.py """

import numpy as np
import pandas as pd
import requests
import json

class EIA(object):
    
    def __init__(self, token: str, series_id: str):
        """Fetch data from www.eia.gov using v2 of the API
        
        Attributes
        ----------
        token: str
            EIA token
        series_id : str
            series id
        
        Methods
        -------
        getData(token: str, series_id: str)
            Fetch data from website and return a pd.Series()
            
        Examples
        -------
        # Get daily prices of natural gas, series id NG.RNGC1.D
        # http://www.eia.gov/beta/api/qb.cfm?category=462457&sdid=NG.RNGC1.D
        token = 'YOUR_EIA_TOKEN'
        nat_gas = 'NG.RNGC1.D'
        eia = EIA(token, nat_gas)
        print(eia.getData())
        """
        self.token = token
        self.series_id = series_id
 
 
    def getData(self) -> pd.Series:
        
        # URL
        url = 'https://api.eia.gov/v2/seriesid/{}?api_key={}'.format(self.series_id.upper(), self.token) 
        
        # Fetch data
        r = requests.get(url)
        jso = r.json()

        # Create dataframe object    
        jso = jso['response']['data']
        df = pd.DataFrame(jso)
        
        # Dates of monthly date come with format Y-m
        # We append the 1st day of the month to get format Y-m-d
        if len(df.period[0]) == 7:
            df.period = df.period.apply(lambda x: x + '-01')
        
        return df

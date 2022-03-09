# """ eia.py """

import numpy as np
import pandas as pd
import requests
import json

class EIA(object):
    
    def __init__(self, token: str, series_id: str):
        """Fetch data from www.eia.gov
        
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
        url = 'https://api.eia.gov/series/?api_key={}&series_id={}'.format(self.token, self.series_id.upper()) 
        
        # Fetch data
        try:
            r = requests.get(url)
            jso = r.json()
            dic = jso['series'][0]['data']

            # Create series object
            lst_dates = np.column_stack(dic)[0]
            lst_values = np.column_stack(dic)[1]
            data = pd.Series(data=lst_values,
                             index=lst_dates)

            # Ensure timestamp format consistency across time frequencies
            if len(data.index[0]) == 4:
                data.index = [x + '0101' for x in data.index]

            if len(data.index[0]) == 6:
                data.index =  [x + '01' for x in data.index]

            data.index = pd.to_datetime(data.index, format='%Y%m%d')
            data.name = self.series_id

            return data

        # Except anything
        except Exception as e:
            print(e)

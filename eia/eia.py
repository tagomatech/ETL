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
        # Construct url
        url = 'http://api.eia.gov/series/?api_key=' + self.token + '&series_id=' + self.series_id.upper()

        try:
            # Fetch data
            r = requests.get(url)
            jso = r.json()
            dic = jso['series'][0]['data']
            lst_dates = np.column_stack(dic)[0]
            lst_values = np.column_stack(dic)[1]

            # Create series object
            data = pd.Series(data=lst_values,
                             index=lst_dates)

            data.index = pd.to_datetime(data.index)
            data.name = self.series_id

            return data
        
        # Except anything
        except Exception as e:
            print(e)

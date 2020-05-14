# nbs.py

import requests, json
import pandas as pd
import numpy as np

class NBS_Fetcher(object):
    """Returns data from China's National Bureau of Statistics (NBS)."""
    
     def __init__(self, database, product, geo='', measure='01', period='LAST13'):
        """
        
        Parameters
        ----------
        database : str
            NBS database to fetch data from. Possible values:
            
            national data, yearly           :   'hgnd'
            national data, quarterly        :   'hgjd'
            national data, monthly          :   'hgyd'
            province data, yearly           :   'fsnd'
            province data, quarterly        :   'fsjd'
            province data, monthly          :   'fsyd'
            city data, yearly               :   'csnd'
            city data, monthly              :   'csyd'
            international data, yearly      :   'gjnd'
            international data, monthly     :   'gjyd'
            3 main countries data, monthly  :   'gjydsdj'
            TODO: Only monthly data at national and province level where dealt with.
            Extend code to other geographical and time granularities
            
        product : str
            Crude oil (production or processing) or oil product (output). Possible values:
            coal                    :   "A030101"
            crude oil               :   "A030102"
            natural gas             :   "A030103"
            coalbed gas             :   "A030104"
            lng                     :   "A030105"
            crude oil processing    :   "A030106" # "Processing Volume of Crude oil" => Runs? Throughput?
            gasoline                :   "A030107"
            kerosene                :   "A030108"
            diesel oil              :   "A030109"
            fuel oil                :   "A03010A"
            naphtha                 :   "A03010B"
            lpg                     :   "A03010C"
            petroleum coke          :   "A03010D"
            asphalt                 :   "A03010E"
            coke                    :   "A03010F"
            electricity             :   "A03010G"
            thermal power           :   "A03010H"
            hydro-electric power    :   "A03010I"
            nuclear power           :   "A03010J"
            wind power              :   "A03010K"
            solar power             :   "A03010L"
            gas                     : "A03010M"

        geo : str, optional
            NBS geographical zone to fetch data for. Possible values:
            Provinces:
                Beijing         :   "110000"
                Tianjin         :   "120000"
                Hebei           :   "130000"
                Shanxi          :   "140000"
                Inner Mongolia  :   "150000"
                Liaoning        :   "210000"
                Jilin           :   "220000"
                Heilongjiang    :   "230000"
                Shanghai        :   "310000"
                Jiangsu         :   "320000"
                Zhejiang        :   "330000"
                Anhui           :   "340000"
                Fujian          :   "350000"
                Jiangxi         :   "360000"
                Shandong        :   "370000"
                Henan           :   "410000"
                Hubei           :   "420000"
                Hunan           :   "430000"
                Guangdong       :   "440000"
                Guangxi         :   "450000"
                Hainan          :   "460000"
                Chongqing       :   "500000"
                Sichuan         :   "510000"
                Guizhou         :   "520000"
                Yunnan          :   "530000"
                Tibet           :   "540000"
                Shaanxi         :   "610000"
                Gansu           :   "620000"
                Qinghai         :   "630000"
                Ningxia         :   "640000"
                Xinjiang        :   "650000"
            TODO: complete the list above for other geographical levels

        measure : str
            Data type required. Possible values:
            Current Period                              :   "01"
            Accumulated                                 :   "02"
            Growth Rate (The same period last year=100) :   "03"
            Accumulated Growth Rate(%)                  :   "04"
            TODO: check data type are always those in the list above across products
            
        period : str
            Timestamp or time range. Includes possible values below:
            13 most recent months   : "LAST13"
            24 most recent months   : "LAST24"
            36 most recent months   : "LAST36"
            Specific year            : "2014", "2015", etc...
            Specific time range     : "2013-2015", "201303-2015-12"
            etc...
            TODO: Review the part of the code that creates the np.Series() object as
            it is likely to break when only 1 data point is returned
            
            
        Returns
        -------
        series
            The time series containing the required data
            
            
        Examples
        --------
        # Example 1 : China gasoline production, monthly volumes (10000 tons) by month from Jun-18 to May-19
        nbs = NBS_Fetcher('hgyd',
                            'A030107',
                            measure='01',
                            period='201806-201905')
        data = nbs.get_data()
        
        # Example 2 : Shandong crude oil processing, monthly growth rate for the past 13 months
        nbs = NBS_Fetcher('hgyd',
                            'A030106',
                            geo='310000',
                            measure='03',
                            period='LAST13')
        data = nbs.get_data()
        
         """
        
        self.database = database
        self.product = product
        self.geo = geo
        self.measure = measure
        self.period = period
        
        # Structure of json returned from NBS server differ depending on the source database
        if self.database[:2] =='hg': # hgyd database (national, monthly data)
            self.i = 1
        elif self.database[:2] == 'fs': # fsyd database (province, monthly data)
            self.i = 2     
        
        # URLs
        url_root ='http://data.stats.gov.cn/english/easyquery.htm'
        self.url_getOtherWds = '{}?m=getOtherWds&dbcode={}&rowcode=zb&colcode=sj&wds=[{{"wdcode":"zb","valuecode":"{}{}"}}]'.format(url_root,
                                                                                                                                    self.database,
                                                                                                                                    self.product,
                                                                                                                                    self.measure)
        self.url_QueryData = '{}?m=QueryData&dbcode={}&rowcode=zb&colcode=sj&wds=[{{"wdcode":"reg","valuecode":"{}"}}]&dfwds=[{{"wdcode":"sj","valuecode":"{}"}}]'.format(url_root,
                                                                                                                                                                        self.database,
                                                                                                                                                                        self.geo,
                                                                                                                                                                        self.period)

    def get_data(self):

        # Fetch data
        with requests.Session() as sess: 
            r_getOtherWds = sess.get(self.url_getOtherWds)
            r_QueryData = sess.get(self.url_QueryData)
        cont = r_QueryData.content

        # Create json
        jso = json.loads(cont.decode('utf8'))

        # Create series
        acc_timestamps = []
        acc_values = []
        for j in jso['returndata']['datanodes']:
            acc_timestamps.append(j['wds'][self.i]['valuecode'])
            if j['data']['data'] == 0:
                acc_values.append(np.nan)
            else:
                acc_values.append(j['data']['data'])
        ser = pd.Series(data=acc_values,
                        index=acc_timestamps)
        ser.index = [pd.to_datetime(ind, format='%Y%m') for ind in ser.index]

        return ser.sort_index()

import gzip
import requests
import re
import numpy as np
import pandas as pd
import datetime as dt

class GSOD(object):
    '''
    Download GSOD weather data from NOAA servers. Also provides additional data facilities.
    
    Parameters
    ----------
    
    station: str, weather station ID. A combination of USAF (6-digit) and
    WBAN (5-digit) identification numbers separated by a dash sign
    
    start: int, default = current year.
    Year to download the data from
    
    end: int, default = current year.
    Year to download the data up to
    
    units: (additional parameter) : str of dic of str. Unit conversion destination,
    e.g. ['celsius', 'km', 'kph', 'cm']   
    '''
    
    def __init__(self, station=None, start=dt.datetime.now().year, end=dt.datetime.now().year, **kwargs):
        self.station = station
        self.start = start
        self.end = end
        self.units = kwargs.get('units')
        # Read weather list of available weather stations
        try:
            print('Wait! Downloading isd-history.csv file from NOAA servers...')
            self.isd_hist = pd.read_csv('http://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv', dtype=object)
                                    
            print('OK!')
            # Rename 'STATION NAME' to 'STATION_NAME'
            self.isd_hist = self.isd_hist.rename(index=str, columns={'STATION NAME' : 'STATION_NAME'})
            
                        
            # To datetime objects
            self.isd_hist.BEGIN = pd.to_datetime(self.isd_hist.BEGIN, format='%Y%m%d', infer_datetime_format=False)
            self.isd_hist.END = pd.to_datetime(self.isd_hist.END, format='%Y%m%d', infer_datetime_format=False)

            # To numeric
            self.isd_hist.LAT = pd.to_numeric(self.isd_hist.LAT)
            self.isd_hist.LON = pd.to_numeric(self.isd_hist.LON)

            # Set index ID as table index
            self.isd_hist['STATION_ID'] = self.isd_hist['USAF'].map(str) + '-' + self.isd_hist['WBAN'].map(str)
            self.isd_hist = self.isd_hist.set_index(self.isd_hist['STATION_ID'])

            # Get rid of useless columns
            self.isd_hist = self.isd_hist.drop(['USAF', 'WBAN', 'ICAO', 'ELEV(M)', 'STATION_ID'], axis=1)
         
            # Headers to lower case
            self.isd_hist.columns = self.isd_hist.columns.str.lower()
          
        except Exception as e:
            print(e)
            
    def station_search(self, select):
        '''
        Parameters
        ----------
        
        select : dict, keys: 'ctry', 'station_name', 'state'
        e.g. {'ctry': 'UK'}, {'state': 'IA'}, {'station_name': 'STANTON'}
        '''
        key = ''.join([k for k in select.keys()])
        val = ''.join([v for v in select.values()])
        return self.isd_hist[self.isd_hist[key] == val]
   
    def getData(self):
        '''
        Get weather data from the internet as memory stream
        '''
        if self.station != None:

            big_df = pd.DataFrame()

            for year in range(self.start, self.end+1):

                # Define URL
                url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(year) + '/' + str(self.station) \
                    + '-' + str(year) + '.op.gz'

                # Define data stream
                stream = requests.get(url)

                # Unzip on-the-fly
                decomp_bytes = gzip.decompress(stream.content)
                data = decomp_bytes.decode('utf-8').split('\n')

                '''
                Data manipulations and ordering
                '''
                # Remove start and end
                data.pop(0) # Remove first line header
                data.pop()  # Remove last element

                # Define lists
                (stn, wban, date, temp, temp_c, dewp, dewp_c,
                 slp, slp_c, stp, stp_c, visib, visib_c,
                 wdsp, wdsp_c, mxspd, gust, max, max_f, min, min_f,
                 prcp, prcp_f, sndp, f, r, s, h, th, tr) = ([] for i in range(30))

                # Fill in lists
                for i in range(0, len(data)):
                    stn.append(data[i][0:6])
                    wban.append(data[i][7:12])
                    date.append(data[i][14:22])         
                    temp.append(data[i][25:30])
                    temp_c.append(data[i][31:33])
                    dewp.append(data[i][36:41])
                    dewp_c.append(data[i][42:44])
                    slp.append(data[i][46:52])      # Mean sea level pressure
                    slp_c.append(data[i][53:55])
                    stp.append(data[i][57:63])      # Mean station pressure
                    stp_c.append(data[i][64:66])
                    visib.append(data[i][68:73])
                    visib_c.append(data[i][74:76])
                    wdsp.append(data[i][78:83])
                    wdsp_c.append(data[i][84:86])
                    mxspd.append(data[i][88:93])
                    gust.append(data[i][95:100])
                    max.append(data[i][103:108])
                    max_f.append(data[i][108])
                    min.append(data[i][111:116])
                    min_f.append(data[i][116])
                    prcp.append(data[i][118:123])
                    prcp_f.append(data[i][123])
                    sndp.append(data[i][125:130])   # Snow depth in inches to tenth
                    f.append(data[i][132])          # Fog
                    r.append(data[i][133])          # Rain or drizzle
                    s.append(data[i][134])          # Snow or ice pallet
                    h.append(data[i][135])          # Hail
                    th.append(data[i][136])         # Thunder
                    tr.append(data[i][137])         # Tornado or funnel cloud

                '''
                Replacements

                min_f & max_f
                blank   : explicit => e
                *       : derived => d
                '''
                max_f = [re.sub(pattern=' ', repl='e', string=x) for x in max_f] # List comprenhension
                max_f = [re.sub(pattern='\*', repl='d', string=x) for x in max_f]

                min_f = [re.sub(pattern=' ', repl='e', string=x) for x in min_f]
                min_f = [re.sub(pattern='\*', repl='d', string=x) for x in min_f]


                '''
                Create dataframe & cleanse data
                '''
                # Create intermediate matrix
                mat = np.matrix(data=[stn, wban, date, temp, temp_c, dewp, dewp_c,
                       slp, slp_c, stp, stp_c, visib, visib_c,
                       wdsp, wdsp_c, mxspd, gust, max, max_f, min, min_f,
                       prcp, prcp_f, sndp, f, r, s, h, th, tr]).T

                # Define header names
                headers = ['stn', 'wban', 'date', 'temp', 'temp_c', 'dewp', 'dewp_c',
                'slp', 'slp_c', 'stp', 'stp_c', 'visib', 'visib_c',
                'wdsp', 'wdsp_c', 'mxspd', 'gust', 'max', 'max_f', 'min', 'min_f',
                'prcp', 'prcp_f', 'sndp', 'f', 'r', 's', 'h', 'th', 'tr']

                # Set precision
                pd.set_option('precision', 3)

                # Create dataframe from matrix object
                df = pd.DataFrame(data=mat, columns=headers)

                # Replace missing values with NAs
                df = df.where(df != ' ', 9999.9)

                # Create station ids
                df['station_id'] = df['stn'].map(str) + '-' + df['wban'].map(str)
                df = df.drop(['stn', 'wban'], axis=1)

                # Convert to numeric
                df[['temp', 'temp_c', 'dewp', 'dewp_c', 'slp', 'slp_c',
                    'stp', 'stp_c', 'visib', 'visib_c', 'wdsp', 'wdsp_c',
                    'mxspd',  'gust', 'max', 'min', 'prcp', 'sndp']] = df[['temp', 'temp_c', 'dewp',
                                                                           'dewp_c', 'slp', 'slp_c', 'stp',
                                                                           'stp_c', 'visib', 'visib_c', 'wdsp',
                                                                           'wdsp_c', 'mxspd', 'gust', 'max',
                                                                           'min', 'prcp', 'sndp']].apply(pd.to_numeric)

                # Replace missing weather data with NaNs
                df = df.replace(to_replace=[99.99, 99.9,999.9,9999.9], value=np.nan)
                
                # Convert to date format
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

                # Set station_id as dataframe index
                df = df.set_index(keys='station_id')

                if self.units:
                    for conv in self.units:

                        if (conv == 'c' or conv == 'C' or conv == 'celsius' or conv == 'Celsius'):
                            df[['temp', 'dewp', 'max', 'min']] = df[['temp', 'dewp', 'max', 'min']].apply(lambda x: (x-32)*5/9)


                        # Convert miles to km
                        elif (conv == 'km' or conv == 'Km' or conv == 'KM'):
                            df['visib'] = df['visib'].apply(lambda x: 1.60934*x)

                        # Convert knots to kph
                        elif (conv == 'kph' or conv == 'KPH'):
                            df[['wdsp', 'mxspd', 'gust']] = df[['wdsp', 'mxspd', 'gust']].apply(lambda x: 1.852*x)

                        # Convert inches to cm
                        elif (conv == 'cm' or conv == 'CM' or conv == 'Cm'):
                            df[['prcp', 'sndp']] = df[['prcp', 'sndp']].apply(lambda x: 2.54*x)

                        else:
                            pass

                big_df = pd.concat([big_df, df])

            return big_df
        
        else:
            print('Require weather station argument!')  

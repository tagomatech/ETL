import gzip
import requests
import re
import numpy as np
import pandas as pd


class GSOD(object):
    def __init__(self, station, year):
        self.station = station
        self.year = year

    def getData(self):

        #####################################################################
        # Get the data from internet as memory stream
        #####################################################################
        #year = 2015
        #station = '040050'
        # Define URL
        url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(self.year) + '/' + str(self.station) \
            + '-' + str(self.year) + '.op.gz'
        #print(url)
        #url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/2015/040050-99999-2015.op.gz'
        #url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/2007/037100-99999-2007.op.gz'
        #url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/1974/170220-99999-1974.op.gz'

        # Define data stream
        stream = requests.get(url)

        # Unzip on-the-fly
        decomp_bytes = gzip.decompress(stream.content)
        data = decomp_bytes.decode('utf-8').split('\n')

        #####################################################################
        # Data manipulations and organization
        #####################################################################

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
            temp.append(data[i][26:30])
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



        #####################################################################
        # Replacements
        #####################################################################

            '''
            min_f & max_f
            blank   : explicit => e
            *       : derived => d
            '''
        max_f = [re.sub(pattern=' ', repl='e', string=x) for x in max_f] # List comprenhension
        max_f = [re.sub(pattern='\*', repl='d', string=x) for x in max_f]

        min_f = [re.sub(pattern=' ', repl='e', string=x) for x in min_f]
        min_f = [re.sub(pattern='\*', repl='d', string=x) for x in min_f]

        prcp_f = [re.sub(pattern='99.99', repl='', string=x) for x in prcp_f]
        
        #####################################################################
        # Create dataframe & data cleansing
        #####################################################################

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

        # Create dataframe from matrix object
        df = pd.DataFrame(data=mat, columns=headers)

        # Replace missing values with NAs
        df = df.where(df != ' ', np.nan)
        #df = df.where(df != '', np.nan)
        #print(df['prcp_f'])
        
        # Create station ids
        df['station'] = df['stn'].map(str) + '-' + df['wban'].map(str)
        df = df.drop(['stn', 'wban'], axis=1)

        # Move station to the left
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('station')))
        df = df.reindex(columns=cols)

        #Convert to numeric
        df[['temp', 'temp_c', 'dewp', 'dewp_c', 'slp', 'slp_c',
            'stp', 'stp_c', 'visib', 'visib_c', 'wdsp', 'wdsp_c',
            'mxspd',  'gust', 'max', 'min', 'prcp', 'sndp']] = df[['temp', 'temp_c', 'dewp',
                                                                   'dewp_c', 'slp', 'slp_c', 'stp',
                                                                   'stp_c', 'visib', 'visib_c', 'wdsp',
                                                                   'wdsp_c', 'mxspd', 'gust', 'max',
                                                                   'min', 'prcp', 'sndp']].apply(pd.to_numeric)

        # Replace missing weather data with NaNs
        df = df.replace(to_replace=[99.99, 99.9,999.9,9999.9, ''], value=np.nan)
        #df[df == 999.9] = np.nan
        #df[df == 9999.9] = np.nan

        # Convert to date format
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # Multi-indexing
        df = df.set_index(keys=['station', 'date'])
        
        #####################################################################
        # Measure conversions
        #####################################################################

        # Convert celsius to fahrenheit
        df[['temp', 'dewp', 'max', 'min']] = df[['temp', 'dewp', 'max', 'min']].apply(lambda x: (x-32)*5/9)

        # Convert miles to km
        df['visib'] = df['visib'].apply(lambda x: 1.60934*x)

        # Convert knots to kph
        df[['wdsp', 'mxspd', 'gust']] = df[['wdsp', 'mxspd', 'gust']].apply(lambda x: 1.852*x)

        # Convert inches to cm
        df[['prcp', 'sndp']] = df[['prcp', 'sndp']].apply(lambda x: 2.54*x)

        # Some specific data cleansing to finish with
        #pd.set_option('precision', 2)
        #df = df.where((pd.notnull(df)), None)
        pd.set_option('display.float_format', lambda x: '%.2f' % x)

        return df



# Test the class above
if __name__ == '__main__':
    '''
    #station = '485680-99999'
    #year = 2015
    gsod = GSOD(station, year)
    data = gsod.getData()
    print(data)
    '''
    # UPDATE database
    from sqlalchemy import create_engine

    stations = ['485680-99999']

    start = 2015
    end = 2015
    years = list(range(start, end + 1))

    eng = create_engine('mysql+mysqlconnector://root:@localhost:3306/gsod')
    con = eng.connect()
   
    for station in stations:
        #print(station)
        for year in years:
            #print(year)
            try:
                # Query table and get the most recent date
                #date = con.execute('SELECT `date` FROM `' + str(station) + '` WHERE `date` IS NOT NULL ORDER BY `date` DESC LIMIT 1')
                dateQuery = con.execute('SELECT MAX(`date`) FROM `' + str(station) + '`')
                from datetime import datetime
                #print((date.fetchone()[0]))
                dateString = dateQuery.fetchone()[0].strftime('%d/%m/%Y')
                date = datetime.strptime(dateString, '%d/%m/%Y')

                gsod = GSOD(station, year)
                data = gsod.getData()
                
                data = data[data.index.levels[1] > date]
                
                #print(data)
                #print((data.index.levels[1]))
                #print(data)
                # Delete all records up to the date before the most recent date in the DB
                #data = data.loc[(data.index.levels[1] > date.fetchone()[0]),['stn', 'wban', 'date', 'temp', 'temp_c', 'dewp', 'dewp_c', 'slp', 'slp_c', 'stp', 'stp_c', 'visib', 'visib_c', 'wdsp', 'wdsp_c', 'mxspd', 'gust', 'max', 'max_f', 'min', 'min_f', 'prcp', 'prcp_f', 'sndp', 'f', 'r', 's', 'h', 'th', 'tr']]
                #data = data[(data.index.levels[1]) > date.fetchone()[0]]
                #print(data)
                #print('Filling {0} for year {1}' % str(station), str(year))
                data.to_sql(name=station, con=con, flavor='mysql', if_exists='append')

                #print(type(date.fetchone()[0]))
                
                #from datetime import datetime
                #print(datetime(2015,10,1,0,0))
                #print(data[data.index.levels[1] > date])

                
            except:
                pass
    con.close() # Does this work? Added after building the wx DB. NOT TESTED

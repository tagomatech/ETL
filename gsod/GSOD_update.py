import gzip
import requests
import re
import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from datetime import datetime

'''
Code below can be enhanced by the implementation of the DBconnectionDriver class
just below, that is finishing/correcting the class itself and use it thereafter
'''

'''
class DBconnectionDriver(object):
    def __init__(self, user, pwd, host, db):
        self.db = db
        self.user = user
        self.host = host
        self.pwd = pwd
    
    def createEngine(self):
        eng = create_engine('mysql+mysqlconnector://%s:%s@localhost:%s/%s' % (self.user, self.pwd,  self.host, self.db))
        return eng

    def openConnection(self):
        open = self.createEngine()
        open.connect()
        #return open

    def closeConnection(self):
        self.openConnection.close()
'''


###############################

thisYear = datetime.now().year

class GSOD(object):
    def __init__(self, year=thisYear, db='gsod', user='root', pwd='', host=3306):
        self.year = year
        #self.station = station
        self.db = db
        self.user = user
        self.pwd = pwd
        self.host = host

    def DBtables(self):
        '''
        # Get database connection
        #con = self.getDBconnection()
        eng = DBconnectionDriver(self.user, self.pwd, self.host, self.db)
        con = eng.openConnection()
        '''
        eng = create_engine('mysql+mysqlconnector://root:%s@localhost:%s/%s' % (self.pwd,  self.host, self.db))
        con = eng.connect()
        tableNamesQuery = con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema="' + self.db + '"')
        con.close()
        tables = []
        for t in tableNamesQuery:
            if t[0] != 'station_data':
                tables.append(t[0])
        return tables


    def getData(self, station):
        #####################################################################
        # Get the data from internet as memory stream
        #####################################################################
  
        # Define URL
        url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(self.year) + '/' + str(station) \
            + '-' + str(self.year) + '.op.gz'

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
        df = df.replace(to_replace=[99.99, 99.9,999.9,9999.9], value=np.nan)   # It doesn't seem to work with
                                                                                    # any column. Maybe a variable
                                                                                    # type issue
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

    def updateGSOD(self):
        '''
        It is the main function in the class. It actually use the two other function defined above:
        - getTables which list all the tables available in the database (default database is GSOD) -> DBTables()
        - fetch data online for all stations in the tables list -> getData()
        - fill the tables with the latest data available
        '''
        # Get the list of all the stations to be updated that is all the tables in the database   
        stationList = self.DBtables()

        # Create database engine
        eng = create_engine('mysql+mysqlconnector://root:%s@localhost:%s/%s' % (self.pwd,  self.host, self.db))
        con = eng.connect()
   
        for station in stationList:
            print('Working on station %s ... ' % str(station))
            try:
                # Query table and get the most recent date and convert it to apporpriate format
                dateQuery = con.execute('SELECT MAX(`date`) FROM `' + str(station) + '`')
                dateString = dateQuery.fetchone()[0].strftime('%d/%m/%Y')
                date = datetime.strptime(dateString, '%d/%m/%Y')

                # Fetch the data from the internet for a given station
                data = self.getData(station=station)

                # Reduce the original table of data to data non available yet in the database
                data = data[data.index.levels[1] > date]

                # Provide information on whether table is already up-to-date
                length = len(data.axes[0])  # axe[0] = vertical
                if length == 0:
                    print('-> Already up-to-date. Data available in the database up to %s\n' % date.strftime('%d/%m/%Y'))
                else:
                   if length >= 1:
                       #print(length)
                       print('-> Updating. Adding %s new row(s) of data in the database.' % (length))
                       print('-> Data now available in the database up to %s\n' % date.strftime('%d/%m/%Y'))
                       data.to_sql(name=station, con=con, flavor='mysql', if_exists='append')
            except:
                pass    # Much than useless. It is dangerous!!

        # Final message -- Oups, there is actually NO garantee all went well!
        print('** Database now updated **')

        # Close database connection
        con.close()


if __name__ == '__main__':
    myGSOD = GSOD()
    myGSOD.updateGSOD()
    

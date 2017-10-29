
'''
Initial program in the GSOD project
It can be used to add new station into the database
'''

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
  
        # Define URL
        url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(self.year) + '/' + str(self.station) \
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

########################################
########################################
from mysql import connector

class addGSODStationToDB(object):
    '''
    This class create a new table that will be filled with GSOD data
    inside an already created database. Default MySQL database is GSOD
    '''
    def __init__(self, station, db='gsod', user='root', pwd='', host='127.0.0.1'):
        self.station = station
        self.db = db
        self.user = user
        self.pwd = pwd
        self.host = host

    def createTable(self):
        '''
        Database connection parameters
        '''
        config = []
        config = {
            'user' : self.user,
            'password' : self.pwd,
            'host' : self.host,
            'database' : self.db,
            'raise_on_warnings': True,
        }
        
        '''
        Database connection
        '''
        try:
            conn = connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your credentials')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
            else:
                print(err)

        '''
        Create table
        '''
        # Create cursor
        curs = conn.cursor()
        
        query = curs.execute('show tables like "' + str(self.station) + '"')
        res = curs.fetchone()
        if res:
            # The table already exists
            print('Table ' + str(self.station) + ' already exists!')
        else:
            try:
                print('Table {%s} was created. Please check history starting date!' % str(self.station) )
                # The table for this station doesn't exist in the darabase
                curs.execute('''
                CREATE TABLE IF NOT EXISTS `''' + station + '''` (
                `station` char(12) NOT NULL,
                `date` date NOT NULL,
                PRIMARY KEY (`station`,`date`),
                KEY `station` (`station`),
                KEY `date` (`date`),
                `temp` float(4,2),
                `temp_c` tinyint,
                `dewp` float(4,2),
                `dewp_c` tinyint,
                `slp` float(6,2),
                `slp_c` tinyint,
                `stp` float(6,2),
                `stp_c` tinyint,
                `visib` float(4,2),
                `visib_c` tinyint,
                `wdsp` float(4,2),
                `wdsp_c` tinyint,
                `mxspd` float(4,2),
                `gust` float(4,2),
                `max` float(4,2),
                `max_f` char(1),
                `min` float(5,2),
                `min_f` char(1),
                `prcp` float(4,2),
                `prcp_f` char(1),
                `sndp` float(5,2),
                `f` char(1),
                `r` char(1),
                `s` char(1),
                `h` char(1),
                `th` char(1),
                `tr` char(1)) ENGINE=InnoDB
                ''')
            except:
                print('Something went wrong while trying to create table ' \
                    + str(self.station) + '!')

########################################
########################################

from sqlalchemy import create_engine

if __name__ == '__main__':    

    stations = ['485680-99999', '485690-99999', '485700-99999', '485800-99999',
       '485830-99999', '486010-99999', '486020-99999', '486030-99999',
       '486150-99999', '486200-99999', '486250-99999', '486470-99999',
       '486570-99999', '486650-99999', '486740-99999', '486940-99999',
       '486980-99999', '960910-99999', '963150-99999', '964130-99999',
       '964210-99999', '964410-99999', '964490-99999', '964650-99999',
       '964710-99999', '964810-99999', '964910-99999', '965050-99999',
       '965090-99999', '965350-99999']
    
    # Starting ending date
    start = 1985 #1985
    end = 2015

    years = list(range(start, end + 1))

    for station in stations:
        try:
            addStation = addGSODStationToDB(station)
            addStation.createTable()
        except:
            pass

    # Create engine and connect to the database
    eng = create_engine('mysql+mysqlconnector://root@localhost:3306/gsod')
    con = eng.connect()

    for station in stations:
        print('Filling MySQL table with data for station %s' % str(station))
        for year in years:
            print('\t\t\t-> Year: %s' % str(year))
            try:
                # Query table and get the most recent date
                gsod = GSOD(station, year)
                data = gsod.getData()
                # Delete all records up to the date before the most recent date in the DB
                #print(data)
                #print('Filling {%s} for year {%s}' % str(station), str(year))
                data.to_sql(name=station, con=con, flavor='mysql', if_exists='append')
            except:
                pass

    print('** Work done! **')

    # Close the connection with the database
    con.close() # Does this work? Added after building the wx DB. NOT TESTED
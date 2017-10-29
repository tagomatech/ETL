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
'''
# Test the class above
if __name__ == '__main__':
    station = '240050-99999'
    addStation = addGSODStationToDB(station=station)
    addStation.createTable()
'''
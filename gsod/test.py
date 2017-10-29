#import gzip
#import requests
#import re
#import numpy as np
#import pandas as pd

from sqlalchemy import create_engine
from datetime import datetime

thisYear = datetime.now().year

class GSOD(object):
    def __init__(self, year=thisYear, db='gsod', user='root', pwd='', host=3306):
        self.year = year
        self.db = db
        self.user = user
        self.pwd = pwd
        self.host = host

    def DBtables(self):
        #eng = create_engine('mysql+mysqlconnector://root@localhost:3306/gsod')# % (self.pwd,  self.host, self.db))
        eng = create_engine('mysql+mysqlconnector://root:%s@localhost:%s/%s' % (self.pwd,  self.host, self.db))
        con = eng.connect()
        tableNamesQuery = con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema="' + self.db + '"')
        #query = con.execute('SELECT * FROM `485680-99999`')
        for t in tableNamesQuery:
            print(t[0])
        #print(query)

if __name__ == '__main__':
    myGSOD = GSOD()
    data = myGSOD.DBtables()


import pandas as pd
import numpy as np
df = pd.DataFrame(np.random.normal(size=(3,4)))
df.axes()
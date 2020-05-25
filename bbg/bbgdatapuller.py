# *- bbgdatapuller.py  *-

import os
import numpy as np
import pandas as pd
import blpapi


class BBGCaller(object):
    '''
    Base class serving as glue to different classes interacting with
    the BBG terminal, but it has no further utility
    '''
    def __init__(self, **kwargs):
        '''
        Parameters
        ----------
        sec : str
            Ticker

        fields : str or list
            Field of list of fields ('PX_HIGH', 'PX_LOW', etc...)

        start : str
            Start date

        end : stf
            End date
        '''
    
    def __init__(self, sec, fields, start=None, end=None):
        #self.rqst = rqst
        self.sec = sec
        self.fields = fields
        self.start = start
        self.end = end


class BBGHistory(BBGCaller):
    '''
    Derived class used to pull historical data from BBG terminal.
    Historical data for a single security can be pulled at a time.
    Menawhile, the number of fields ('PX_HIGH', 'PX_LOW', etc...) at each call
    is not limited.

    TODO:
        * Allow to download data for different tickers.

        * For the time being data frequency is BBG default, most of the time
        daily. An improvement would be to allow frequency change.


    Returns
    -------

    data : pd.DataFrame()
        The historical data queried returned in a dataFrame presented as
        long format


    '''
    def __init__(self, sec, fields, start, end):
        super().__init__(sec, fields, start, end)

            
    def get_data(self):
            # Session management
            sess = blpapi.Session()
            sess.start()

            # Define data type
            sess.openService('//blp/refdata')
            service = sess.getService('//blp/refdata')

            # Create request
            request = service.createRequest('HistoricalDataRequest')

           # Optional request setters
            request.set('startDate', self.start)
            request.set('endDate', self.end)
            request.getElement('securities').appendValue(self.sec)

            # Data holders 
            date_acc =[]
            ticker_acc = []
            field_acc = []
            value_acc = []

            # Loop over fields
            for fie in self.fields:
                request.getElement('fields').appendValue(fie)
            sess.sendRequest(request)
            endReached = False
            while endReached == False:
                event = sess.nextEvent(500)
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    for msg in event:
                        fieldData = msg.getElement('securityData').getElement('fieldData')
                    for data in fieldData.values():
                        for fld in self.fields:
                            date_acc.append(data.getElement('date').getValue())
                            field_acc.append(fld)
                            value_acc.append(data.getElement(fld).getValue())
                            ticker_acc.append(self.sec)
                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    endReached = True
            sess.stop()
                
            data = pd.DataFrame({'timestamp' : date_acc,
                                   'ticker' : ticker_acc,
                                   'field' : fie,
                                   'value' : value_acc})

            return data

'''
# Use example of BBGHistory
from bbgdatapuller import BBGHistory # Expect folder issue
security = 'SIMA SW Equity' #'SIMA SW Equity'
fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST']
#fields = ['PX_OPEN']
start = '20200105'
end = '20200109'

d = BBGHistory(sec=security, fields=fields, start=start, end=end).get_data()
d.head()
'''

# *- bbgdailyhistory.py -*

import os
import numpy as np
import pandas as pd
import blpapi


class BBGDailyHistory:
    '''
    Parameters
    ----------
    sec : str
        Ticker
    fields : str or list
        Field of list of fields ('PX_HIGH', 'PX_LOW', etc...)
    start : str
        Start date
    end : str
        End date
    '''
    
    def __init__(self, sec, fields, start=None, end=None):
        #self.rqst = rqst
        self.sec = sec
        self.fields = fields
        self.start = start
        self.end = end

    
    def get_data(self) -> pd.DataFrame:
        '''
        Returns
        -------
        data : pd.DataFrame()
            The historical data queried returned in a dataFrame presented as
            long format
        '''
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
                            'field' : field_acc,
                            'value' : value_acc})

        return data


'''
# Example
if __name__ == "__main__":
    # Use example of BBGHistory
    #from bbgdatapuller import BBGHistory # Expect folder issue
    security = 'SIMA SW Equity' #'SIMA SW Equity'
    fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST']
    start = '20200105'
    end = '20200109'
    d = BBGDailyHistory(sec=security, fields=fields, start=start, end=end).get_data()
    print(d.head())
'''

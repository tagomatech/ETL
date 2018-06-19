import nass, json
import pandas as pd

class crop_data(object):
    '''
    Input arguments:
        - USDA website query:
            - crop      : corn, soybean, wheat
            - year_start   : starting year
            - year_end     : ending year
            - geo       :  US, or abreviated US States names (OH, IA, ...)
    '''

    def __init__(self, crop, geo, year_start, year_end=None):
        self.crop = crop
        self.year_start = year_start
        if year_end == None:
            self.year_end = year_start
        else:
            self.year_end = year_end
        self.geo = geo

    def __repr__(self):
        return 'Crop: {%s}\nStart: {%s}\nEnd: {%s}\nGeo:{%s}\n' % (self.crop, self.year_start, self.year_end, self.geo)

 
class fetch_usda(crop_data):
    def __init__(self, api, description, crop, geo, year_start, year_end=None):
        self.api = api
        self.description = description # crop progress or condition ('progress', 'condition')
        super().__init__(crop, geo, year_start, year_end=None)

    def __repr__(self):
        return super().__repr__() + 'API: {%s}\n' % self.api

    def fetch(self):
        # Create and connect to the API
        api = nass.NassApi(self.api)
        query = api.query()

        # Create data query
        query.filter('source_desc','SURVEY')\
        .filter('sector_desc','CROPS')\
        .filter('group_desc','FIELD CROPS')\
        .filter('commodity_desc',self.crop)\
        .filter('domain_desc','TOTAL')\
        .filter('year',range(self.year_start,self.year_end+1))\
        .filter('freq_desc','WEEKLY')

        if self.description == 'CONDITION':
            query.filter('util_practice_desc','ALL UTILIZATION PRACTICES')
            query.filter('statisticcat_desc',['CONDITION', 'CONDITION, 5 YEAR AVG',
                                    'CONDITION, PREVIOUS YEAR'])
            query.filter('unit_desc',['PCT EXCELLENT','PCT GOOD', 'PCT FAIR',
                                     'PCT POOR', 'PCT VERY POOR'])
        
        else: # ie, self.description == 'PROGRESS'
            query.filter('util_practice_desc',['GRAIN', 'ALL UTILIZATION PRACTICES', 'BULK'])
            query.filter('statisticcat_desc',['PROGRESS', 'PROGRESS, 5 YEAR AVG',
                                             'PROGRESS, PREVIOUS YEAR'])
            query.filter('unit_desc',['PCT PLANTED', 'PCT HARVESTED',
                             'PCT DENTED', 'PCT DOUGH', 'PCT EMERGED',
                             'PCT MATURE', 'PCT MILK', 'PCT SILKING',
                             'PCT BLOOMING', 'PCT COLORING',  'PCT DROPPING LEAVES',
                             'PCT EMERGED', 'PCT SETTING POD', 'PCT FULLY PODDED',
                             'PCT MATURE', 'PCT BOOTED', 'PCT JOINTING', 'PCT HEADED',
                             'PCT COLORING', 'PCT MATURE'])

        #planted, emerged jointed headed coloring mature harvested

        #if self.crop == 'CORN':
        #    query.filter('util_practice_desc',['ALL UTILIZATION PRACTICES','GRAIN'])

        if self.geo == 'US':
            query.filter('agg_level_desc','NATIONAL')
        else:
            query.filter('agg_level_desc', 'STATE').filter('state_alpha', self.geo)

        # Execute query
        query_exec = query.execute()

        # Create dataframe
        dumped_data = json.dumps(query_exec)
        df = pd.read_json(path_or_buf=dumped_data)

        if self.geo == 'US':
            df = df.rename(columns = {'agg_level_desc':'geo'})
        else:
            df = df.rename(columns = {'state_name':'geo'})
            '''
            # keep:
            commodity_desc
            Value
            geo <= agg_level_desc if national level
            geo <= state_name  if State level
            begin_code
            unit_desc
            year
            week_ending
            
            # drop:
            cv (%)
            asd_code
            asd_desc
            class_desc
            congr_district_code
            country_code
            state_fips_code
            state_name
            statisticcat_desc
            util_practice_desc
            watershed_code  watershed_desc
            zip_5
            '''
        df = df[['commodity_desc', 'year', 'week_ending', 'geo', 'begin_code', 'unit_desc', 'Value']]

        df.columns = ['crop', 'year', 'date', 'geo', 'week', 'attribute', 'value']

        return df

'''
test = fetch_usda(api='B84157F6-1D75-34F0-85B1-ABD3C164D8CA', // https://quickstats.nass.usda.gov/api
                  description='PROGRESS',
                  crop='WHEAT',
                  geo= 'KS',
                  year_start=2015) 
  
f = test.fetch()
'''


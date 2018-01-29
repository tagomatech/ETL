import datetime as dt
import nass
import json
import pandas as pd

class crop_prog(object):

    def __init__(self, crop, api=api, start=dt.datetime.now().year, end=dt.datetime.now().year, geo='US'):
        self.api = nass.NassApi(api)
        self.crop = crop
        self.start = start
        self.end = end
        self.geo = geo
    
    def get_data(self):
    	'''
	TODO:
	Check whether additional features/filters are required depending on e.g. the
	agricultural product selected
    	'''

        # Establish connexion
        q = self.api.query()
        
        # Data source
        q.filter('source_desc','SURVEY')
        
        # Data segment
        q.filter('sector_desc','CROPS')
        q.filter('group_desc','FIELD CROPS')
        
        # Filter by crop
        q.filter('commodity_desc', self.crop)
        
        # Crop condition & progress
        q.filter('statisticcat_desc',['CONDITION', 'CONDITION, 5 YEAR AVG', 'CONDITION, PREVIOUS YEAR',
                                    'PROGRESS', 'PROGRESS, 5 YEAR AVG', 'PROGRESS, PREVIOUS YEAR'])
        
        q.filter('unit_desc',['PCT EXCELLENT','PCT GOOD','PCT FAIR',
                             'PCT POOR','PCT VERY POOR',
                             'PCT PLANTED', 'PCT HARVESTED',
                             'PCT DENTED', 'PCT DOUGH', 'PCT EMERGED',
                             'PCT MATURE', 'PCT MILK', 'PCT SILKING',
                             'PCT BLOOMING', 'PCT COLORING',  'PCT DROPPING LEAVES',
                             'PCT EMERGED', 'PCT SETTING PODS', 'PCT FULLY PODDED', 'PCT MATURE'])
                            
        '''
        q.filter('unit_desc',['PCT 2 TO 4 INCHES', 'PCT ABOVE NORMAL', 'PCT ACTIVE', 'PCT ADEQUATE', 'PCT BELOW NORMAL', 'PCT BLOOMING',
         'PCT BOLLS OPENING', 'PCT BOOTED', 'PCT BREAKING DORMANCY', 'PCT BT SIZE GROUP', 'PCT BY DISTANCE',
         'PCT BY METHOD', 'PCT BY OUTLET', 'PCT BY PRACTICE', 'PCT BY SIZE GROUP', 'PCT BY TYPE', 'PCT BY YEARS',
         'PCT CALVED', 'PCT CLOSED MIDDLES (ROWS FILLED)', 'PCT COLORING', 'PCT COMPLETE', 'PCT CUT',
         'PCT DEFOLIATED', 'PCT DENTED', 'PCT DIFFICULT', 'PCT DORMANT', 'PCT DOUGH', 'PCT DROPPING LEAVES',
         'PCT DUG', 'PCT EMERGED', 'PCT EXCELLENT', 'PCT FAIR', 'PCT FINISHED', 'PCT FROM PASTURES', 'PCT FRUIT SET',
         'PCT FULL BLOOM', 'PCT FULLY PODDED', 'PCT GOOD', 'PCT GREEN TIP', 'PCT GT 4 INCHES', 'PCT HARVESTED',
         'PCT HEADED', 'PCT HEAVY', 'PCT INACCESSIBLE', 'PCT JOINTING', 'PCT LAMBED', 'PCT LIGHT', 'PCT LT 2 INCHES',
         'PCT MATURE', 'PCT MILK', 'PCT MODERATE', 'PCT NONE', 'PCT NORMAL', 'PCT NOT READY FOR STRIPPING',
         'PCT OF AG LAND', 'PCT OF APPLICATIONS', 'PCT OF AREA BEARING & NON-BEARING', 'PCT OF AREA BEARING, 10TH PERCENTILE',
         'PCT OF AREA BEARING, 90TH PERCENTILE', 'PCT OF AREA BEARING, AVG', 'PCT OF AREA BEARING, CV PCT',
         'PCT OF AREA BEARING, MEDIAN', 'PCT OF AREA NON-BEARING, AVG', 'PCT OF AREA PLANTED', 'PCT OF AREA PLANTED, 10TH PERCENTILE',
         'PCT OF AREA PLANTED, 90TH PERCENTILE', 'PCT OF AREA PLANTED, AVG', 'PCT OF AREA PLANTED, CV PCT', 'PCT OF AREA PLANTED, MEDIAN',
         'PCT OF COLONIES', 'PCT OF COMMERCIAL', 'PCT OF COMMODITY TOTALS', 'PCT OF CROP & ANIMAL WORKERS', 'PCT OF EXPENSE',
         'PCT OF FARM OPERATIONS', 'PCT OF FARM SALES', 'PCT OF FUEL EXPENSES', 'PCT OF HIRED & AG SERVICE WORKERS',
         'PCT OF HIRED WORKERS', 'PCT OF INVENTORY', 'PCT OF LB CERTIFIED & POST-MORTEM CONDEMNED', 'PCT OF MKTG YEAR',
         'PCT OF OPERATING EXPENSES', 'PCT OF OPERATIONS', 'PCT OF OPERATIONS BY METHOD', 'PCT OF ORGANIC SALES',
         'PCT OF PARITY', 'PCT OF POULTS PLACED', 'PCT OF PRODUCTION EXPENSES', 'PCT OF RETAIL & WHOLESALE',
         'PCT OF SLAUGHTER', 'PCT OF TOTAL EXPENSES', 'PCT OF TOTAL STOCKS', 'PCT OF VOLUME HANDLED, AVG',
         'PCT PASTURED', 'PCT PEGGING', 'PCT PETAL FALL', 'PCT PINK', 'PCT PLANTED', 'PCT POOR', 'PCT RAY FLOWERS DRIED OR DROPPED',
         'PCT READILY ACCESSIBLE', 'PCT READY FOR STRIPPING', 'PCT SEEDBED PREPARED', 'PCT SETTING BOLLS',
         'PCT SETTING PODS', 'PCT SEVERE', 'PCT SHORN', 'PCT SHORT','PCT SILKING', 'PCT SQUARING', 'PCT STRIPPED',
         'PCT SURPLUS', 'PCT TOPPED', 'PCT TRANSPLANTED', 'PCT TURNING BROWN', 'PCT TURNING YELLOW', 'PCT VERY POOR',
         'PCT VERY SHORT', 'PCT VINES DRY'])
        '''
                
        # Domain
        q.filter('domain_desc','TOTAL')
        
        # Geographical parameter: State(s) or US
        if self.geo != 'US':
            q.filter('agg_level_desc','STATE')
            q.filter('state_alpha', self.geo)
        else:
            q.filter('agg_level_desc', 'NATIONAL')
        
        # Time period 
        q.filter('year',range(self.start,self.end+1))
        
        # Data frequency
        q.filter('freq_desc','WEEKLY')
        
        # Additional filtering (required?)
        #q.filter('util_practice_desc','GRAIN')
        
        # Execute query
        exec_query = q.execute()

	# Re-arrange data into a dataframe        
        dumped = json.dumps(exec_query)
        df = pd.read_json(path_or_buf=dumped)
        df = df.set_index(['week_ending'])
        df = df.drop(['agg_level_desc', 'asd_code', 'asd_desc', 'class_desc', 'congr_district_code', 'country_name',
                      'county_ansi', 'county_code', 'county_name', 'country_code', 'CV (%)',
                      'domain_desc', 'domaincat_desc', 'group_desc', 'end_code', 'freq_desc',
                      'location_desc', 'load_time', 'prodn_practice_desc', 'reference_period_desc', 'region_desc', 'sector_desc', 'source_desc',
                      'state_ansi', 'state_fips_code', 'state_name',
                      'statisticcat_desc', 'util_practice_desc', 'unit_desc', 'watershed_code', 'watershed_desc',
                      'zip_5'], axis=1)
                
        # Drop corn silage related ratings, if any
        df = df[~df.short_desc.str.contains('SILAGE')]
        
        # Rename columns
        df = df.rename(index=str, columns={'begin_code': 'week',
                                           'commodity_desc': 'crop',
                                           'short_desc': 'rating',
                                           'state_alpha': 'geo',
                                           'Value': 'val'})

        df = df.replace({'(?:\w*) - CONDITION, MEASURED IN PCT EXCELLENT': 'e',
                         '(?:\w*) - CONDITION, MEASURED IN PCT FAIR': 'f',
                         '(?:\w*) - CONDITION, MEASURED IN PCT GOOD': 'g',
                         '(?:\w*) - CONDITION, MEASURED IN PCT POOR': 'p',
                         '(?:\w*) - CONDITION, MEASURED IN PCT VERY POOR': 'vp',
                         
                         '(?:\w*) - CONDITION, PREVIOUS YEAR, MEASURED IN PCT EXCELLENT': 'e_prev_year',
                         '(?:\w*) - CONDITION, PREVIOUS YEAR, MEASURED IN PCT FAIR': 'f_prev_year',
                         '(?:\w*) - CONDITION, PREVIOUS YEAR, MEASURED IN PCT GOOD': 'g_prev_year',
                         '(?:\w*) - CONDITION, PREVIOUS YEAR, MEASURED IN PCT POOR': 'p_prev_year',
                         '(?:\w*) - CONDITION, PREVIOUS YEAR, MEASURED IN PCT VERY POOR': 'vp_prev_year',
                         
                         '(?:\w*) - CONDITION, 5 YEAR AVG, MEASURED IN PCT EXCELLENT': 'e_5y_ave',
                         '(?:\w*) - CONDITION, 5 YEAR AVG, MEASURED IN PCT FAIR': 'f_5y_ave',
                         '(?:\w*) - CONDITION, 5 YEAR AVG, MEASURED IN PCT GOOD': 'g_5y_ave',
                         '(?:\w*) - CONDITION, 5 YEAR AVG, MEASURED IN PCT POOR': 'p_5y_ave',
                         '(?:\w*) - CONDITION, 5 YEAR AVG, MEASURED IN PCT VERY POOR': 'vp_5y_ave',
                         
                         '(?:(\w*,\s\w*|\w*)) - PROGRESS, MEASURED IN PCT HARVESTED': 'harvested',                     
                         '(?:(\w*,\s\w*|\w*)) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT HARVESTED': 'harvested_prev_year',
                         '(?:(\w*,\s\w*|\w*)) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT HARVESTED': 'harvested_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT DENTED': 'dent',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT DENTED': 'dent_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT DENTED': 'dent_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT DOUGH': 'dough',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT DOUGH': 'dough_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT DOUGH': 'dough_5y_ave',

                         '(?:\w*) - PROGRESS, MEASURED IN PCT SILKING': 'silk',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT SILKING': 'silk_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT SILKING': 'silk_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT EMERGED': 'emerge',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT EMERGED': 'emerge_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT EMERGED': 'emerge_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT MATURE': 'matu',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT MATURE': 'matu_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT MATURE': 'matu_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT PLANTED': 'planted',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT PLANTED': 'planted_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT PLANTED': 'planted_5y_ave',
                        
                         '(?:\w*) - PROGRESS, MEASURED IN PCT BLOOMING': 'bloom',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT BLOOMING': 'bloom_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT BLOOMING': 'bloom_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT SETTING PODS': 'pod',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT SETTING PODS': 'pod_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT SETTING PODS': 'pod_5y_ave',
                         
                         '(?:\w*) - PROGRESS, MEASURED IN PCT DROPPING LEAVES': 'drop_leav',                     
                         '(?:\w*) - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT DROPPING LEAVES': 'drop_leav_prev_year',
                         '(?:\w*) - PROGRESS, 5 YEAR AVG, MEASURED IN PCT DROPPING LEAVES': 'drop_leav_5y_ave'
                        
                        }, regex=True)
        return df

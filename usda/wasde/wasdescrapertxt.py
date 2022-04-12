# wasdescrappertxt.py
# tagoma Apr22

import datetime as dt
import numpy as np
import pandas as pd
import re
import requests


class WASDE:
    
    def __init__(self, url:str, report_date:str):# -> None:
        self.url = url
        self.report_date = report_date
        self.wasde = requests.get(self.url).text

    
    @staticmethod
    def extract_page(report:str, page_pattern:str) -> str:
        pattern = re.compile('WASDE\s+-\s+\d{{3,4}}\s+-\s+\d{{1,2}}\s+\w+\s\d{{4}}\\r\\n\s+\\r\\n{commo}\\r\\n\=+\\r\\n[\s\S]*?\=+[\s\S]*?\=+\\r\\n'.format(commo = page_pattern))
        match = re.search(pattern, report)
        return match.group()


    @staticmethod
    def regex(pattern_name:str, string:str) -> list:
        
        dic_patterns = {'num_std' : re.compile('(\d+(?:\.\d+)?)(?!\/)|(?=\w+)NA(?!\w+)'), #re.compile('(\d+(?:\.\d+)?)(?!\/)'), #re.compile('(\d+(?:\.\d+)?)'),
                        'report_nb'  : re.compile('(?:-\s+)(\d{3,4})(?:\s+-)'),
                        'report_date' : re.compile('\w+\s+\d{4}'),
                        'mkg_campaign': re.compile('(\d{4}\/\d{2}\s+Est.|\d{4}\/\d{2}\s+Proj.|\d{4}\/\d{2})'),
                        'proj_date': re.compile('(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)') # TODO: Can do better
        }
        
        return re.findall(dic_patterns[pattern_name], string)


    @staticmethod
    def reorder_columns(df:pd.DataFrame) -> pd.DataFrame:

        columns = ['WasdeNumber', 'ReportDate', 'ReportTitle', 'Attribute', 'ReliabilityProjection',
                'Commodity', 'Region', 'MarketYear', 'ProjEstFlag', 'AnnualQuarterFlag', 'Value',
                'Unit', 'ReleaseDate', 'ReleaseTime', 'ForecastYear', 'ForecastMonth']

        return df[columns]


    def extract_us_wheat(self) -> pd.DataFrame:
        pattern_us_wheat = r'\s+U.S.\s+Wheat\s+Supply\s+and\s+Use\s+1\/'
        string_us_wheat =  self.extract_page(self.wasde, pattern_us_wheat)
        lst_us_wheat = string_us_wheat.split('\r\n')

        dic_lines_tbl_main = {8 : 'num_std', # 'Area Planted
                        9 : 'num_std', # Area Harvested
                        12 : 'num_std', # Yield per Harvested Acre
                        15 : 'num_std', # Beginning Stocks
                        16 : 'num_std', # Production
                        17 : 'num_std', # Imports
                        18 : 'num_std', # Supply, Total
                        19 : 'num_std', # Food
                        20 : 'num_std', # Seed
                        21 : 'num_std', # Feed and Residual
                        22 : 'num_std', # Domestic, Total
                        23 : 'num_std', # Exports
                        24 : 'num_std', # Use, Total
                        25 : 'num_std', # Ending Stocks
                        26 : 'num_std', # Avg.FarmPrice ($/bu)
        }
        
        lst_col_histo = []
        lst_col_est = []
        #lst_col_proj_old = []
        lst_col_proj_new = []

        for row, pat in dic_lines_tbl_main.items():
            reg_res = self.regex(pat, lst_us_wheat[row])
            lst_col_histo.append(reg_res[0])
            lst_col_est.append(reg_res[1])
            #lst_col_proj_old.append(reg_res[2])
            lst_col_proj_new.append(reg_res[3])

        # Marketing campaigns
        lst_mkg_years = self.regex('mkg_campaign', lst_us_wheat[4])

        ## Projection datea
        #lst_proj_dates = self.regex('proj_date', lst_us_wheat[5])

        ## Tweak marketing campaigns name (append projection dates)
        #lst_mkg_years[2] = lst_mkg_years[2] + ' ' + lst_proj_dates[0]
        #lst_mkg_years[3] = lst_mkg_years[3] + ' ' + lst_proj_dates[1]

        #arr_values = np.array([lst_col_histo, lst_col_est, lst_col_proj_old, lst_col_proj_new])
        arr_values = np.array([lst_col_histo, lst_col_est, lst_col_proj_new])
        df = pd.DataFrame(data=arr_values.T, columns=[lst_mkg_years[0], lst_mkg_years[1], lst_mkg_years[3]])

        # Report number
        wasde_nb = ''.join(self.regex('report_nb', lst_us_wheat[0]))
        df.loc[ : , 'WasdeNumber'] = [wasde_nb] * len(dic_lines_tbl_main)

        # Report date
        report_date = ''.join(self.regex('report_date', lst_us_wheat[0]))
        df.loc[ : , 'ReportDate'] = [report_date] * len(dic_lines_tbl_main)

        # Attribute
        df.loc[ : , 'Attribute'] =  ['Area Planted', 'Area Harvested', 'Yield per Harvested Acre',  'Beginning Stocks', 'Production',
                    'Imports', 'Supply, Total', 'Food', 'Seed', 'Feed and Residual', 'Domestic, Total',
                    'Exports', 'Use, Total', 'Ending Stocks', 'Avg. Farm Price']

        # Units
        df.loc[ : , 'Unit'] = ['Million Acres'] * 2 + ['Bushels'] + ['Million Bushels'] * 11 + ['$/bu']

        # ReportTitle
        df.loc[ : , 'ReportTitle'] = 'U.S. Wheat Supply and Use'

        # ReliabilityProjection
        df.loc[ : , 'ReliabilityProjection'] = np.nan

        # Commodity
        df.loc[ : , 'Commodity'] = 'Wheat'

        # Region
        df.loc[ : , 'Region'] = 'United States'

        # AnnualQuarterFlag
        df.loc[ : , 'AnnualQuarterFlag'] = 'Annual'
        


        # TODO: MarketYear	ProjEstFlag
        id_vars = list(set(df.columns.tolist()) - set(lst_mkg_years))

        df_melted = df.melt(id_vars=id_vars, value_vars=[lst_mkg_years[0], lst_mkg_years[1], lst_mkg_years[3]], var_name='MarketYear', value_name='Value')

        df_melted.loc[ : , 'ProjEstFlag'] = df_melted.MarketYear.str.extract(r'((?:\s)\w{3,4}\.$)')

        df_melted.loc[ : , 'MarketYear_NEW'] = df_melted.MarketYear.str.extract(r'(\d{4}\/\d{2})')
        df_melted = df_melted.drop(columns=['MarketYear'])
        df_melted = df_melted.rename(columns={'MarketYear_NEW' : 'MarketYear'})


        # TODO :
        # - get 'ForecastYear' and 'ForecastMonth' from user argument publication date
        # - move 'ReleaseDate', 'ReleaseTime', 'ForecastYear', 'ForecastMonth' to a function (this is dependent on the whole report not on US wheat)
   
        # Forecast year
        report_date_complete = '1 ' + report_date
        # lst_report_dates = report_date.split(' ')
        report_date_dt = dt.datetime.strptime(report_date_complete, '%d %B %Y').date()
        df_melted.loc[ : , 'ForecastYear'] = report_date_dt.year#

        # Forecast month
        df_melted.loc[ : , 'ForecastMonth'] = report_date_dt.month#

        # Release time
        df_melted.loc[ : , 'ReleaseTime'] = '12:00:00.0000000'# Convert to datetime?

        # Release date
        df_melted.loc[ : , 'ReleaseDate'] = self.report_date#

        return self.reorder_columns(df_melted)


"""
Demo: parse a set of WASDE and extract the US wheat balance

def main():

    df = pd.DataFrame()
    publi_date = '1900-01-01' # Irrelevant here for the demo

    lst_wasde_urls = ['https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/jw828h37k/qb98nk075/wasde0422.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/fj237573x/4b29c983s/wasde0322.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/w0893f10d/2514pn640/wasde0222.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/pv63h1937/s1785p515/wasde0122.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/m039m632s/td96m352w/wasde1221.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/wp989j89k/t148gj30v/wasde1121.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/cc08jf37b/6d570x89h/wasde1021.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/gf06h129z/c247fr540/wasde0921.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/9019t127d/v405t727t/wasde0821.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/kh04fk547/3197zh727/latest.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/2514pf89m/wp989f04n/latest.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/h702r223s/1544cj198/latest.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/dn39xt93w/bc387c841/latest.txt',
                'https://downloads.usda.library.cornell.edu/usda-esmis/files/3t945q76s/dv140m16d/np194335s/latest.txt']


    cnt_errors = 0
    cnt_reports = 0
    lst_failed_reports = []

    start_time = dt.datetime.now()

    for url in lst_wasde_urls:
        
        try:
            wasde = WASDE(url, publi_date)
            us_wheat = wasde.extract_us_wheat()
            df = pd.concat([df, us_wheat])
            cnt_reports += 1
        
        except:
            cnt_errors += 1
            lst_failed_reports.append(url)
    
    end_time =  dt.datetime.now()

    print('Time elapsed {} seconds'.format((end_time - start_time).total_seconds()))
    print('Number of errors {} over {} reports'.format(cnt_errors, cnt_reports + cnt_errors))
    print('Reports failed: {}'.format(lst_failed_reports if len(lst_failed_reports) > 0 else 'None'))
    print('Dataframe shape:\t{}'.format(df.shape[0]))
    print(df.head())


if __name__ == '__main__':
    main()

'''
Time elapsed 1.497003 seconds
Number of errors 0 over 14 reports
Reports failed: None
Dataframe shape:        630       
  WasdeNumber  ReportDate                ReportTitle                 Attribute  ReliabilityProjection Commodity         Region  ... AnnualQuarterFlag Value             Unit ReleaseDate       ReleaseTime ForecastYear ForecastMonth
0         623  April 2022  U.S. Wheat Supply and Use              Area Planted                    NaN     Wheat  United States  ...            Annual  45.5    Million Acres  1900-01-01  12:00:00.0000000         2022             4        
1         623  April 2022  U.S. Wheat Supply and Use            Area Harvested                    NaN     Wheat  United States  ...            Annual  37.4    Million Acres  1900-01-01  12:00:00.0000000         2022             4        
2         623  April 2022  U.S. Wheat Supply and Use  Yield per Harvested Acre                    NaN     Wheat  United States  ...            Annual  51.7          Bushels  1900-01-01  12:00:00.0000000         2022             4        
3         623  April 2022  U.S. Wheat Supply and Use          Beginning Stocks                    NaN     Wheat  United States  ...            Annual  1080  Million Bushels  1900-01-01  12:00:00.0000000         2022             4        
4         623  April 2022  U.S. Wheat Supply and Use                Production                    NaN     Wheat  United States  ...            Annual  1932  Million Bushels  1900-01-01  12:00:00.0000000         2022             4
'''

"""

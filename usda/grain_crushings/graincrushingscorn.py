# graincrushingcorn.py
# Aug-21
# tagoma

import requests
import re
import datetime as dt
import dateutil.relativedelta
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer


def get_file_content_from_url(url):
    req = requests.get(url)
    # A bit of cleaning
    pattern_cleaner = '[\\n|z\\r]'
    string_cleaned = re.sub(pattern_cleaner, ' ', req.text)
    
    return string_cleaned


def get_corn_table(full_text):
    pattern_table_corn = '(?<=Dry and Wet Mill, Corn Consumed).+(?=Dry Mill, Sorghum Consumed)'
    comp_table_corn = re.compile(pattern_table_corn,  re.MULTILINE)
    res_table_corn = re.search(comp_table_corn, full_text)[0]
    
    pattern_current_month = '(?<=United States: )\w+ \d{4}'
    comp_current_month = re.compile(pattern_current_month,  re.MULTILINE)
    res_current_month = re.search(comp_current_month, full_text)[0]
    
    # Dates
    current_month_dt = dt.datetime.strptime(res_current_month, '%B %Y')
    previous_month_dt = current_month_dt - dateutil.relativedelta.relativedelta(months=1)
    previous_year_dt = current_month_dt - dateutil.relativedelta.relativedelta(months=12)
    
    pattern_data_corn = '(\d{1,3},\d{1,3}\s+\d{1,3},\d{1,3}\s+\d{1,3},\d{1,3}|\(D\)\s+\d{1,3},\d{1,3}\s+\d{1,3},\d{1,3}|\d{1,3},\d{1,3}\s+\(D\)\s+\d{1,3},\d{1,3}|\d{1,3},\d{1,3}\s+\d{1,3},\d{1,3}\s+\(D\))'
    comp_data_corn = re.compile(pattern_data_corn,  re.MULTILINE)
    res_data_corn = re.findall(comp_data_corn, res_table_corn)

    # Delete comma as thd separator
    res_data_corn = re.sub(',', '', str(res_data_corn))
    res_data_corn = re.sub("' '", "', '", str(res_data_corn))

    # Delete single quotes
    res_data_corn = re.sub("'", '', str(res_data_corn))
    res_data_corn = re.sub("' '", "', '", str(res_data_corn))

    # Replace wite spaces with commas
    res_data_corn = re.sub('\s+', ',', str(res_data_corn))
    res_data_corn = re.sub("' '", "', '", str(res_data_corn))

    res_data_corn = res_data_corn[1:-1].split(',')

    res_data_corn = [x for x in res_data_corn if x != ''] # Remove possible blank values
    #res_data_corn = [int(el) for el in res_data_corn]

    # Corn table base
    dic_purposes = [{'purpose1' : 'Consumed for alcohol production', 'purpose2' : 'Beverage alcohol', 'purpose3' : '', 'purpose4' : ''},
                    {'purpose1' : 'Consumed for alcohol production', 'purpose2' : 'Fuel alcohol', 'purpose3' : '' , 'purpose4' : ''},
                    {'purpose1' : 'Consumed for alcohol production', 'purpose2' : 'Fuel alcohol', 'purpose3' : 'Dry mill' , 'purpose4' : ''},
                    {'purpose1' : 'Consumed for alcohol production', 'purpose2' : 'Fuel alcohol', 'purpose3' : 'Wet mill' , 'purpose4' : ''},
                    {'purpose1' : 'Consumed for alcohol production', 'purpose2' : 'Industrial alcohol' , 'purpose3' : '' , 'purpose4' : ''},
                    {'purpose1' : 'Consumed for other purposes', 'purpose2' : 'Total wet mill products other than fuel' , 'purpose3' : '' , 'purpose4' : ''}]
    df_corn = pd.DataFrame(dic_purposes)
    
    # Create final datafame
    lst_dates = [previous_year_dt, previous_month_dt, current_month_dt]
    df_merged = pd.DataFrame()
    i = 0
    for start in range(3):
        lst_loop_dates = []
        lst_loop_values = []
        for j in range(start, len(res_data_corn), 3):
            lst_loop_dates.append(lst_dates[i])
            lst_loop_values.append(res_data_corn[j])
        i +=1
        df_corn.loc[:, 'timestamp'] = lst_loop_dates
        df_corn.loc[:, 'value'] = lst_loop_values
        df_merged = pd.concat([df_merged, df_corn])

    return df_merged


def get_dates_and_urls(nb_of_pages=8):
    '''
    Return the URLs of the different Grain Crushings and Co-Products Production
    reports as well as their release dates
    '''
    lst_release_dates = []
    lst_urls = []
    for p in range(1, nb_of_pages+2):
        url = 'https://usda.library.cornell.edu/concern/publications/n583xt96p?locale=en&page={}#release-items'.format(p)
        response = requests.get(url)

        limitation = SoupStrainer('tbody', {'id': 'release-items'})
        for resp in BeautifulSoup(response.content, parse_only=limitation):
            for a in resp.find_all('a', {'href': re.compile(r'\.txt')}):
                date_dt = dt.datetime.strptime(a['data-release-date'],"%Y-%m-%dT%H:%M:%SZ")
                date_str = date_dt.strftime('%Y-%m-%d')
                
                '''
                Remove unwanted reports
                '''
                # Notice regarding reschedule
                pattern_remove_1 = 'Reschedule'
                comp_remove_1 = re.compile(pattern_remove_1,  re.MULTILINE)
                res_remove_1 = re.search(comp_remove_1, a['href'])
                if res_remove_1:
                    continue
                    
                # Report of Feb 19, 2015 is the oldest one. It contains data for 4Q2014
                # Table layout is different from other reports
                if date_dt <= dt.datetime(2015, 2, 28):
                    continue
                
                
                lst_release_dates.append(date_str)
                lst_urls.append(a['href'])
                
    return lst_urls, lst_release_dates


'''
Now get the thing running
'''
# Get URLs and release dates
urls, release_dates = get_dates_and_urls()

# Loop over reports and get data for CORN ethanol
df = pd.DataFrame()
for url, release_date in zip(urls, release_dates):
    print(release_date, '\t', url)
    content = get_file_content_from_url(url)
    table = get_corn_table(content)
    
    table.loc[:, 'release_date'] = release_date
    table.loc[:, 'url'] = url
    
    df = pd.concat([df, table])   

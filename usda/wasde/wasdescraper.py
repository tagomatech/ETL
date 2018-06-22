import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

class Wasde(object):
    
    def __init__(self, url):
        self.url = url
    
    def _getSoup(self):
        resp = requests.get(self.url).text
        return BeautifulSoup(resp, 'lxml')
        
    @staticmethod
    def _getMaketingYears12_1(soup, page, table):
        acc = []
        for mkg_yr in range(4):
            acc.append(soup.find('sr' + str(page))\
                    .report\
                    .find('matrix' + str(table))\
                    .find('m' + str(table) + '_attribute_group')\
                    .m1_filler1\
                    .find_all('m' + str(table) + '_year_group')[mkg_yr]['market_year1'])
        return acc
    
    @staticmethod
    def _getMaketingYears15_1(soup, page, table):
        acc = []
        for mkg_yr in range(4):
            acc.append(soup.find('sr' + str(page))\
                    .report\
                    .find('matrix' + str(table))\
                    .find('m1_year_group_collection')\
                    .find_all('m1_year_group')[mkg_yr]['market_year4'])
        return acc
        
    @staticmethod
    def _getProjMths12_1(soup, page, table):
        # Return 3-letter mth proj. prev. mth [2] and proj. curr. mth [3];
        # Index [0] and [1] must be None
        acc = []
        for mkg_yr in range(4):
            acc.append(soup.find('sr' + str(page))\
                       .report.find('matrix' + str(table))\
                       .find('m' + str(table) + '_attribute_group')\
                       .find('m' + str(table) + '_filler1')\
                       .find_all('m' + str(table) + '_year_group')[mkg_yr]\
                       .m1_month_group['forecast_month1'])
        return acc
    
    @staticmethod
    def _getProjMths15_1(soup, page, table):
        # Return 3-letter month proj. prev. month [2] and proj. curr. month [3];
        # Index [0] and [1] must be None
        acc = []
        for mkg_yr in range(4):
            acc.append(soup.find('sr' + str(page))\
                .report.find('matrix' + str(table))\
                .find('m' + str(table) + '_attribute_group')\
                .find('m' + str(table) + '_filler1')\
                .find_all('m' + str(table) + '_year_group')[mkg_yr]\
                .find('m' + str(table) + '_month_group')['forecast_month4'])
        return acc
    
    @staticmethod
    def _getItNb(soup, page, table):
    # Get the number of items (rows) in the pagele
        return len(soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('m' + str(table) + '_attribute_group'))
    
    @staticmethod
    def _getItNb15_2(soup, page, table):
        return len(soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('attribute5'))
    
    @staticmethod
    def _getItNb15_3(soup, page, table):
        return len(soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('attribute6'))

    @staticmethod
    def _getIts12_2(soup, page, table, item):
    # Return flow name
        return soup.find('sr' + str(page))\
                    .report\
                    .find('matrix' + str(table))\
                    .find_all('m' + str(table) + '_attribute_group')[item]\
                    .attribute2['attribute2']
    
    @staticmethod
    def _getIts15_1(soup, page, table, item):
        # Return flow name
        return soup.find('sr' + str(page))\
                    .report\
                    .find('matrix' + str(table))\
                    .find_all('m1_attribute_group')[item]\
                    .attribute4['attribute4']
    
    @staticmethod
    def _getIts15_2(soup, page, table, item):
        return soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('m2_attribute_group')[item]\
                   .attribute5['attribute5']
        
    @staticmethod
    def _getIts15_3(soup, page, table, item):
        return soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('attribute6')[item]['attribute6']

    @staticmethod
    def _getVals12_2(soup, page, table, item, mkg_yr):
        return soup.find('sr' + str(page))\
                    .report.find('matrix' + str(table))\
                    .find_all('attribute2')[item]\
                    .find_all('m2_month_group')[mkg_yr]\
                    .cell['cell_value2']

    @staticmethod
    def _getVals15_1(soup, page, table, item, mkg_yr):
        return soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                    .find_all('attribute4')[item]\
                    .find_all('m1_month_group')[mkg_yr]\
                     .cell['cell_value4']
    
    @staticmethod
    def _getVals15_2(soup, page, table, item, mkg_yr):
        return soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('attribute5')[item]\
                   .find_all('m2_month_group')[mkg_yr]\
                   .cell['cell_value5']
    
    @staticmethod
    def _getVals15_3(soup, page, table, item, mkg_yr):
        return soup.find('sr' + str(page))\
                   .report.find('matrix' + str(table))\
                   .find_all('attribute6')[item]\
                   .find_all('m3_month_group')[mkg_yr]\
                   .cell['cell_value6']

    @staticmethod
    def _getMonthReport(soup):
        return soup.sr08.report['report_month']

    @staticmethod
    def _dfFactory(mkg_yrs, pr_mths, item_nb, items, act_vals, est_vals, pm_proj_vals, cm_proj_vals, report_date):
 
        df_act_vals = pd.DataFrame({'marketing_year' : np.repeat(mkg_yrs[0], item_nb),
                                    'statistics_month' : np.repeat(pr_mths[0], item_nb),
                                    'flow' : items,
                                    'estimate_type' : 'actual',
                                    'value' : act_vals})

        df_est_vals = pd.DataFrame({'marketing_year' : np.repeat(mkg_yrs[1], item_nb),
                                    'statistics_month' : np.repeat(pr_mths[1], item_nb),
                                    'flow' : items,
                                    'estimate_type' : 'estimate',
                                    'value' : est_vals})
        
       
        df_pm_proj_vals = pd.DataFrame({'marketing_year' : np.repeat(mkg_yrs[2], item_nb),
                                        'statistics_month' : np.repeat(pr_mths[2], item_nb),
                                        'flow' : items,
                                        'estimate_type' : 'prev. proj.',        
                                        'value' : pm_proj_vals})
        
        df_cm_proj_vals = pd.DataFrame({'marketing_year' : np.repeat(mkg_yrs[3], item_nb),
                                        'statistics_month' : np.repeat(pr_mths[3], item_nb),
                                        'flow' : items,
                                        'estimate_type' : 'new proj.',
                                        'value' : cm_proj_vals})
                           
        df = pd.concat([df_act_vals, df_est_vals, df_pm_proj_vals, df_cm_proj_vals])

        df['update'] = report_date
       
        return df
    
  
    def USSoybean(self):
        soup = self._getSoup()
        
        # Marketing years - ['2016/17', '2017/18 Est.', '2018/19 Proj.', '2018/19 Proj.']
        mkg_yrs = self._getMaketingYears15_1(soup, 15, 1)

        # Projected mths - ['', '', 'Apr', 'May']             
        pr_mths = self._getProjMths15_1(soup, 15, 1)

        # Number of items in the table - 15
        item_nb = self._getItNb(soup, 15, 1)

        # Report
        report = np.repeat(soup.sr08.report.get('report_month'), 4*item_nb)
        
        # Items - ['Area Planted', 'Area Harvested', 'Yield per Harvested Acre', ... 'Avg. Farm Price ($/bu)  4/']
        items = [self._getIts15_1(soup, 15, 1, item) for item in range(item_nb)]

        act_vals_acc = []
        est_vals_acc = []
        pm_pr_acc = []
        cm_pr_acc = []
               
        for mkg_yr in range(4):
            for item in range(item_nb):
                if mkg_yr == 0:
                    act_vals_acc.append(self._getVals15_1(soup, 15, 1, item, mkg_yr))
                elif mkg_yr == 1:
                    est_vals_acc.append(self._getVals15_1(soup, 15, 1, item, mkg_yr))
                elif mkg_yr == 2:
                    pm_pr_acc.append(self._getVals15_1(soup, 15, 1, item, mkg_yr))
                else:
                    cm_pr_acc.append(self._getVals15_1(soup, 15, 1, item, mkg_yr))

        report_date = self._getMonthReport(soup)

        return self._dfFactory(mkg_yrs, pr_mths, item_nb, items, act_vals_acc, est_vals_acc, pm_pr_acc, cm_pr_acc, report_date)
    
    
    def USSoymeal(self):
        soup = self._getSoup()
        mkg_yrs = self._getMaketingYears15_1(soup, 15, 1)
        pr_mths = self._getProjMths15_1(soup, 15, 1)
        item_nb = self._getItNb15_3(soup, 15, 3)
        items = [self._getIts15_3(soup, 15, 3, item) for item in range(item_nb)]

        act_vals_acc = []
        est_vals_acc = []
        pm_pr_acc = []
        cm_pr_acc = []
               
        for mkg_yr in range(4):
            for item in range(item_nb):
                if mkg_yr == 0:
                    act_vals_acc.append(self._getVals15_3(soup, 15, 3, item, mkg_yr))
                elif mkg_yr == 1:
                    est_vals_acc.append(self._getVals15_3(soup, 15, 3, item, mkg_yr))
                elif mkg_yr == 2:
                    pm_pr_acc.append(self._getVals15_3(soup, 15, 3, item, mkg_yr))
                else:
                    cm_pr_acc.append(self._getVals15_3(soup, 15, 3, item, mkg_yr))

        report_date = self._getMonthReport(soup)

        return self._dfFactory(mkg_yrs, pr_mths, item_nb, items, act_vals_acc, est_vals_acc, pm_pr_acc, cm_pr_acc, report_date)
 

    def USSoyoil(self):
        soup = self._getSoup()
        mkg_yrs = self._getMaketingYears15_1(soup, 15, 1)
        pr_mths = self._getProjMths15_1(soup, 15, 1)
        item_nb = self._getItNb15_2(soup, 15, 2)
        items = [self._getIts15_2(soup, 15, 2, item) for item in range(item_nb)]

        act_vals_acc = []
        est_vals_acc = []
        pm_pr_acc = []
        cm_pr_acc = []
               
        for mkg_yr in range(4):
            for item in range(item_nb):
                if mkg_yr == 0:
                    act_vals_acc.append(self._getVals15_2(soup, 15, 2, item, mkg_yr))
                elif mkg_yr == 1:
                    est_vals_acc.append(self._getVals15_2(soup, 15, 2, item, mkg_yr))
                elif mkg_yr == 2:
                    pm_pr_acc.append(self._getVals15_2(soup, 15, 2, item, mkg_yr))
                else:
                    cm_pr_acc.append(self._getVals15_2(soup, 15, 2, item, mkg_yr))

        report_date = self._getMonthReport(soup)

        return self._dfFactory(mkg_yrs, pr_mths, item_nb, items, act_vals_acc, est_vals_acc, pm_pr_acc, cm_pr_acc, report_date)


    def USCorn(self):
        soup = self._getSoup()
        mkg_yrs = self._getMaketingYears12_1(soup, 12, 1)
        pr_mths = self._getProjMths12_1(soup, 12, 1)
        item_nb = self._getItNb(soup, 12, 2)
        items = [self._getIts12_2(soup, 12, 2, item) for item in range(item_nb)]

        act_vals_acc = []
        est_vals_acc = []
        pm_pr_acc = []
        cm_pr_acc = []
               
        for mkg_yr in range(4):
            for item in range(item_nb):
                if mkg_yr == 0:
                    act_vals_acc.append(self._getVals12_2(soup, 12, 2, item, mkg_yr))
                elif mkg_yr == 1:
                    est_vals_acc.append(self._getVals12_2(soup, 12, 2, item, mkg_yr))
                elif mkg_yr == 2:
                    pm_pr_acc.append(self._getVals12_2(soup, 12, 2, item, mkg_yr))
                else:
                    cm_pr_acc.append(self._getVals12_2(soup, 12, 2, item, mkg_yr))


        report_date = self._getMonthReport(soup)

        return self._dfFactory(mkg_yrs, pr_mths, item_nb, items, act_vals_acc, est_vals_acc, pm_pr_acc, cm_pr_acc, report_date)


'''
Copyright (C) 2016, Edouard 'tagoma' Tallent
Updating spreadsheet gasoline and ethanol weekly prices
with data fetched from http://www.anp.gov.br/preco/
QuantCorner @ https://quantcorner.wordpress.com
'''

'''
This class requires additional Python libraries such as pandas and
selenium. It also requires Google Chrome, ChromeDriver and Excel
to be installed on the local machine.
'''
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import re
from xlwings import Workbook, Sheet, Range
from datetime import timedelta
from datetime import datetime


class ANP(object):
    '''
    Initialize variables
    '''
    def __init__(self, granular=1, fuel=1):
        self.granularInd = granular
        self.fuelInd = fuel
      
    '''
    Define the Selenium driver that is Chrome here
    '''
    def driver(self):
        self.driver = webdriver.Chrome()
        self.driver.get('http://www.anp.gov.br/preco/prc/Resumo_Quatro_Index.asp')
     
    def selectState(self):
        '''
        Select radio button among BRASIL, REGIÕES, ESTADOS, and
        MUNICÍPIO with:
        0: BRASIL
        1: REGIÕES
        2: ESTADOS
        3: MUNICÍPIO => not this item, as it implies a new field in the webpage
        '''
        granularXpath = '''//input[@type='radio'][@name='rdResumo'][@id='rdResumo''''' \
            + str(self.granularInd) + '''']'''
        granular = WebDriverWait(self.driver, 8).until(lambda driver: \
            self.driver.find_element_by_xpath(granularXpath)) # 8 secs wait
        granular.click()


    def selectfuel(self):
        '''
        Select fuel
        1: Gasolina
        2: Ethanol
        3: GNV
        4: Diesel
        5: Diesel@S10
        6: GLP
        '''
        fuelXpath = '''//select[@id='selCombustivel']/option[''' + str(self.fuelInd) \
            + ']'
        fuel = WebDriverWait(self.driver, 8).until(lambda driver: \
            self.driver.find_element_by_xpath(fuelXpath)) # 8 secs wait
        fuel.click()

    def captcha(self):
        '''
        Read the captcha and fill in the input box
        '''
        # Read the captcha 
        captcha = []
        for label in range(1,5):
            labelXpath = '''//div[@id='divQuadro']/label[''' + str(label) + ']'
            capt = WebDriverWait(self.driver, 8).until(lambda driver: \
            self.driver.find_element_by_xpath(labelXpath)) # 8 secs wait
            captcha.append(capt.text)
        # Fill in the input box
        boxXpath = '''//input[@id='txtValor'][@name='txtValor'][@type='text']'''
        writer = self.driver.find_element_by_xpath(boxXpath)    
        writer.clear() # Clear possible unwanted user input
        writer.send_keys(''.join(captcha))
        
    def submit(self):
        submitXpath = '''//div[@class='bts_form']/button[@id='image1']'''
        submit = WebDriverWait(self.driver, 8).until(lambda driver: \
            driver.find_element_by_xpath(submitXpath)) # 8 seconds wait
        submit.click()
        submit.click() # For some reasons a single click doesn't work

        # Check if the new webpage was actually loaded
        checkXpath = '//h3[3]'
        checkText = 'Período : Quatro últimas semanas'
        check = WebDriverWait(self.driver, 8).until(lambda driver: \
            driver.find_element_by_xpath(checkXpath)) # 8 seconds wait
        if check.text != checkText:
            print('Problem accessing the webpage!')
        else:
            pass
            #print ('Page check: OK!')

    def sendToXL(self):
        # Return the table contained in the new webpage
        # Standard driver.page_source cannot work here
        trAcc = []
        for trInd in range (4, 24):
            trXpath = '//table[1]/tbody/tr[''' + str(trInd) + ']/td'
            tr = WebDriverWait(self.driver, 8).until(lambda driver: \
                driver.find_elements_by_xpath(trXpath)) # 8 seconds wait
            tdAcc = []
            for i in range (0, 12):
                tdAcc.append(tr[i].text)
            trAcc.append(tdAcc)

        df = pd.DataFrame(trAcc, columns=['state', 'date', 'nb_obs', 'mid_prc_cons', 'dev_cons', 'min_prc_cons', 'max_prc_cons', 'ave_margin', 'mid_prc_distr', 'dev_distr', 'min_prc_distr', 'max_prc_distr'])
        
        regex = lambda x: re.sub('\d{1,2}/\d{2}/\d{4}-', '', x)
        df['date'] = df['date'].apply(regex)
        df['date'] = pd.to_datetime(df['date'],format='%d/%m/%Y')

        df = df[['state', 'date', 'mid_prc_cons', 'mid_prc_distr']]

        '''
        Deal with the workbook
        '''
        try:
            wb = Workbook('Daily Light Ends Indicators_DRAFT.xlsb')
        except:
            wb = Workbook('C:/Users/Ted/Desktop/Daily Light Ends Indicators_DRAFT.xlsb') 
        #wb = Workbook('Daily Light Ends Indicators_DRAFT.xlsb') # New in xlwings0.6.0. Doesn't reopen already opened workbook 
             
        # Last Value in the date column in the spreadsheet 'Brazil EtOH vs gasoline prices'...
        lastRow = 44
        while True:
            if Range('Brazil EtOH vs gasoline prices', 'C'+str(lastRow)).value != None:
                lastRow+=1
            else:
                break
        
        mostRecentDateXL = Range('Brazil EtOH vs gasoline prices', 'C'+str(lastRow)).offset(-1,0).value

        dateXLtoCompare =  mostRecentDateXL + timedelta(days=6)

        states = ['Centro Oeste', 'Nordeste', 'Norte', 'Sudeste', 'Sul']

        toSendToXL = df[df['date'] > dateXLtoCompare]

        endDate = int(len(toSendToXL)/5)

        
        hOffset = 0
        vOffset = 0

        if self.fuelInd == 1:
            col = 'E'
        elif self.fuelInd == 2:
            col = 'D'
            # Fill the date column
            for x in range(0, endDate):
                Range('Brazil EtOH vs gasoline prices', 'C' + str(lastRow)).offset(x,0).value = toSendToXL['date'].iloc[x]
        else:
            pass
     
        for state in states:
            for x in range(0, endDate):
                    Range('Brazil EtOH vs gasoline prices', col + str(lastRow)).offset(vOffset, hOffset).value = toSendToXL['mid_prc_cons'][toSendToXL['state'] == state].iloc[x]
                    vOffset+=1
            hOffset += 2
            vOffset = 0

    def quit(self):
        self.driver.quit()
 


if __name__ == '__main__':

    # Ethanol
    eth = ANP()  # 1: ethanol
    eth.driver()
    eth.selectState()
    eth.selectfuel()
    eth.captcha()
    eth.submit()
    eth.sendToXL()
    eth.quit()

    # Gasoline
    gas = ANP(fuel=2)  # 2: gasoline
    gas.driver()
    gas.selectState()
    gas.selectfuel()
    gas.captcha()
    gas.submit()
    gas.sendToXL()
    gas.quit()

   

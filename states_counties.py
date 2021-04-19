import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import datetime
import numpy as np
import os
import sys

mydir = os.path.expanduser("~/GitHub/StateCovidData/")
mydir2 = os.path.expanduser("~/GitHub/SupplyDemand/")

dates = []
def dataframe():
    JH_DATA_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
    df_main = pd.DataFrame(columns = ['Province/State', 'Country/Region', 'date',
                                'Lat' , 'Long', 'Confirmed', 'Deaths', 'Recovered'])
    
    
    today = pd.Timestamp('today')
    today = '{:%m-%d-%Y}'.format(today)
    
    
    dates = pd.date_range(start='3-10-2020', end=today, freq='d')
    dates = pd.to_datetime(dates, format='%m-%d-%Y')
    dates = dates.strftime('%m-%d-%Y').tolist()
    
    for date in dates:
        fname = JH_DATA_URL + date + '.csv'
        
        try:
            df = pd.read_csv(fname)
        except:
            continue
        
        d1 = datetime.datetime.strptime(date, "%m-%d-%Y")
        d2 = datetime.datetime.strptime('03-22-2020', "%m-%d-%Y")
        
        if d1 < d2:
            try:
                df = df[df['Country/Region'] == 'US']
                df['date'] = date
                df = df.filter(['Admin2', 'Province/State', 'Country/Region', 'date',
                                'Lat' , 'Long', 'Confirmed', 'Deaths', 'Recovered'])
                
            except:
                pass
            
        else:
            try:
                df = df[df['Country_Region'] == 'US']
                df['date'] = date
                df = df.filter(['Admin2', 'Province_State', 'Country_Region', 'date', 
                                'Lat' , 'Long_', 'Confirmed', 'Deaths', 'Recovered'])
                
                df.columns = ['Admin2', 'Province/State', 'Country/Region', 'date',
                                'Lat' , 'Long', 'Confirmed', 'Deaths', 'Recovered']
            except:
                pass
            
        df_main = pd.concat([df_main, df])
    
    df_main['date'] = pd.to_datetime(df_main['date'])
    df_main['date'] = df_main['date'].dt.strftime('%m/%d/%Y')

    try:
        df_sums = df_main.drop(['Admin2'], axis=1)
    except:
        pass
    
    df_sums = df_main.groupby(['Province/State','date'])['Confirmed'].sum().reset_index()
    
    return df_sums, df_main, dates
    

df_sums, df_main, dates = dataframe()

df = df_main.filter(items=['Province/State', 'Admin2'], axis=1)
df.drop_duplicates(inplace=True)

df = df[~df['Admin2'].isin(['Unassigned', 'Out-of-state', 
                            'Out of AL', 'Out of IL',
                            'Out of CO', 'Out of GA',
                            'Out of HI', 'Out of LA',
                            'Out of ME', 'Out of MI',
                            'Out of OK', 'Out of PR',
                            'Out of TN', 'Out of UT',
                            '', None, np.nan,
                            ])]

print(df.shape)

df.to_csv(mydir2 + 'DataUpdate/data/States_Counties.txt', sep='\t')



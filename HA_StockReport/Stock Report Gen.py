from msilib.schema import Directory
import pandas as pd
import numpy as np
import os
from datetime import date
today = date.today()
user = os.getlogin()

day = today.strftime("%Y%m%d")
input_directory = 'C:/Users/{}/Downloads/Python Program/HA_StockReport/raw'.format(os.getlogin())
adword_directory = 'C:/Users/{}}/Downloads/Python Program/HA_StockReport/adword_raw'.format(os.getlogin())
output_directory = 'C:/Users/{}}/Downloads/Python Program/HA_StockReport/output'.format(os.getlogin())
ha_list = ['Washer','Washer_Dryer','Refrigerator','Dryer','Steam_Clothing_Care_System','Vacuum_Cleaner','Air_Conditioner','Dishwasher','Cooking_Appliance','HA_Others','Air_Care_Solution','Filtered_Water_Dispenser','pralki-i-suszarki','lodowki','szafy-parowe','mikrofale','zmywarki']
country_allcat = ['GB','PL']
it_cat = ['Refrigerator','Washer_Dryer','Washer']

os.makedirs(input_directory)
os.makedirs(adword_directory)
os.makedirs(output_directory)


def create_data(inputder,adwordder,outputder):
    #GMC DF
    df = pd.DataFrame() 

    for file in os.listdir(input_directory):
        df1 = pd.read_csv(os.path.join(input_directory,file),sep='\t')
        #if df1.iloc[2,'country']== 'PL':
        #    df1['brand'] = df1['custom label 0']
        df = pd.concat([df,df1])

    df = df[['country','title','item group id','id','brand','availability']]
    df = df[df['brand'].isin(ha_list)]
    df['bu'] = 'ha'
    df['item group id'] = df['item group id'].apply(lambda x:str(x).split('.')[0].lower())
    df['counrty & ID'] = df['country'] + '_' + df['item group id']

    #Adwords DF
    for file in os.listdir(adword_directory):
        aw_df = pd.read_csv(os.path.join(adword_directory,file),sep='\t',encoding='utf-16')
    aw_df = aw_df[aw_df['Campaign'].str.contains('assembly',case=False)] #Assembly Only
    aw_df['item group id'] = aw_df[aw_df['Product Group Type'] == 'Biddable']['Product Group'].apply(lambda x:x.split("'")[-2].split('.')[0].lower())
    aw_df = aw_df[['Product Group Type','item group id']]

    #join
    finaldf = df.merge(aw_df,how='left',on='item group id')
    finaldf['google shopping'] = np.where((finaldf['country'] == 'IT') & (finaldf['brand'].isin(it_cat)),'yes',
                                 np.where(finaldf['country'].isin(country_allcat),'yes',
                                 np.where(finaldf['Product Group Type'].notnull(),'yes','no')))
    finaldf.drop('Product Group Type',inplace=True,axis=1)
    finaldf.rename(columns ={'item group id':'SKU'}, inplace=True)

    return df,finaldf

df, finaldf = create_data(input_directory,adword_directory,output_directory)

finaldf.to_csv(os.path.join(output_directory,'{}.csv'.format(today)), encoding='utf-8',index=False)

input_directory
from asyncio.windows_events import NULL
from msilib.schema import Directory
import pandas as pd
import numpy as np
import os


## Variable 
input_filename = 'final_raw_de.csv'
category_raw = 'Web - Page Category2(149)'
cat_location = 3
include_word = ['de:']
remove_word = ['brand','offer','CATEGORY','about','cs:','2021',':business:','blog','magazine','cloud','test1','2020','2019','career','microsite','promotion','Charity']
output_name = 'output.csv'

###############################################################################

input_directory = 'C:/Users/{}/Downloads/{}'.format(os.getlogin(),input_filename)

df_raw = pd.read_csv(input_directory,sep=',')

def filter_df(df,column,word_array,include=False):
    if include == False:
        for string in word_array:
            df = df[~df[column].str.contains(string)]
        df_output = df
    else:
        df_output = pd.DataFrame()
        for string in word_array:
            df1 = df[df[column].str.contains(string)]
            df_output = pd.concat([df_output,df1])
            df_output.drop_duplicates(inplace=True)
    return df_output

#df = df_raw[df_raw['Web - Page Category2(149)'].str.contains(country + ':')]

df = filter_df(df_raw,category_raw,include_word,include=True) #Filter for Country
df = filter_df(df,category_raw,remove_word1,include=False) #Filter out Non product page

# 체크용 df['Web - Page Category2(149)'].unique()

df['Category'] = df.loc[:,category_raw].apply(lambda x:x.split(':')[cat_location-1].lower() if len(x.split(':')) >= cat_location else NULL) #Getting Category data from category_raw column

df = df[df['Category'] != 0] #None 0 Category

df_filtered = df.loc[:,['Web - Client ID(102)','Category']]

df_join = pd.merge(df_filtered,df_filtered,on=['Web - Client ID(102)'])


df_output = df_join.groupby(['Category_x','Category_y'],as_index=False).nunique() #Count unique CID
df_output= df_output[df_output['Category_x'] <= df_output['Category_y']]
df_output.sort_values(by='Web - Client ID(102)',ascending=False,inplace=True)
df_output.to_csv('C:/Users/jeremy.tang/Downloads/{}'.format(output_name),index=False)

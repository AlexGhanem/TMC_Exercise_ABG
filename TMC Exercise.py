import os
import pandas as pd
from Levenshtein import distance as lev

#reading the data from the voterfile csv and input data
df_vf = pd.DataFrame()
for root, dirs, files in os.walk('./Data'):
    for name in files:
        if (name.endswith(".csv") and name != 'eng-matching-input-v3.csv'):
            df_vf = pd.concat([df_vf,pd.read_csv(root+'/'+name)],ignore_index=True)
df_matching = pd.read_csv('./Data/eng-matching-input-v3.csv')
df_matching.drop('row',axis=1,inplace=True)

#formatting input data and creating a new dataframe with relevant data

df_matching['name'] = df_matching['name'].str.lower()

columns = [
    'SOS_VOTERID','FIRST_NAME',
    'MIDDLE_NAME','LAST_NAME',
    'DATE_OF_BIRTH',
    'RESIDENTIAL_ADDRESS1',
    'RESIDENTIAL_ZIP',
    'MAILING_ZIP',
    ]
df = df_vf.reindex(columns=columns)
df['name'] = df['FIRST_NAME'].astype(str) + ' ' + df['MIDDLE_NAME'].astype(str) + ' ' + df['LAST_NAME'].astype(str)
df['name']=df['name'].str.lower()

df['RESIDENTIAL_ADDRESS1']=df['RESIDENTIAL_ADDRESS1'].str.lower()

df['birth_year'] = df['DATE_OF_BIRTH'].astype(str).str.split('-',expand=True)[0]

#calculating levenshtein distance between each inputed name and voterfile name
#result is a table that houses relevant data from both inputted and voterfile data
#this process takes up to 20 minutes

ls=[]
for i in df_matching.iterrows():
    for j in df.iterrows():
        name_vf = j[1]['name']
        vf_id = j[1]['SOS_VOTERID']
        vf_birth_year = j[1]['birth_year']
        name_entry = i[1]['name']
        entry_birth_year = i[1]['birth_year']
        entry_index = i[0]
        distance = lev(name_entry,name_vf)
        score = {
            'name_vf':name_vf, 
            'name_entry':name_entry,
            
            'distance':distance,
            'vf_id':vf_id,
            'entry_index':entry_index,

            'vf_birth_year':vf_birth_year,
            'entry_birth_year':entry_birth_year
            }
        ls.append(score)
        if distance == 0:
            break

#instantiating and formatting output table 

df_distance_scores = pd.DataFrame(ls)

df_distance_scores['entry_birth_year'] = df_distance_scores['entry_birth_year'].astype("Int64")
df_distance_scores['vf_birth_year'] = df_distance_scores['vf_birth_year'].astype("Int64")

#bringing in zipcode data from the file. 
df_scores_zip = df_distance_scores.merge(df_matching['zip'], left_on='entry_index', right_index=True).merge(df_vf[['RESIDENTIAL_ZIP','SOS_VOTERID']], left_on='vf_id',right_on='SOS_VOTERID')   

df_scores_zip.drop('SOS_VOTERID',axis=1,inplace=True)

#filtering the dataframe to include people with equal zipcodes in both sets
df_address_match = df_scores_zip[(df_scores_zip['zip']== df_scores_zip['RESIDENTIAL_ZIP'])].sort_values(['distance','name_entry'])

"""decision tree algorithm that iterates over the created 
table and appends when certain criteria are met.
(see chart in ReadMe for logic)
"""
ls=[]
for i in df_address_match.iterrows():
    entry_last_name = i[1]['name_entry'].split()[-1]
    vf_last_name = i[1]['name_vf'].split()[-1]
    entry_first_name = i[1]['name_entry'].split()[0]
    vf_first_name = i[1]['name_vf'].split()[0]
    vf_birth_year = i[1]['vf_birth_year']
    entry_birth_year = i[1]['entry_birth_year']

    if len(i[1]['name_entry'].split()) > 2:
        entry_middle_name = i[1]['name_entry'].split()[1]
    else:
        entry_middle_name = pd.NA

    if len(i[1]['name_vf'].split()) > 2:
        vf_middle_name = i[1]['name_vf'].split()[1]
    else:
        vf_middle_name = pd.NA
    

    if entry_last_name == vf_last_name and entry_first_name[0] == vf_first_name[0]:
        if(len(entry_first_name)>1):
            if(entry_first_name==vf_first_name):
                if not pd.isna(vf_middle_name) and not pd.isna(entry_middle_name):
                    if(vf_middle_name[0] == entry_middle_name[0]):
                        if(not pd.isna(entry_birth_year)):
                            if((entry_birth_year==vf_birth_year)):
                                ls.append(i[1])
                else:
                    if(not pd.isna(entry_birth_year)):
                        if((entry_birth_year==vf_birth_year)):
                            ls.append(i[1])
                    else: 
                        ls.append(i[1])

        elif(entry_first_name[0]==vf_first_name[0]):
                if not pd.isna(vf_middle_name) and not pd.isna(entry_middle_name):
                    if(vf_middle_name[0] == entry_middle_name[0]):
                        if(not pd.isna(entry_birth_year)):
                            if((entry_birth_year==vf_birth_year)):
                                ls.append(i[1])
                        else:
                            ls.append(i[1])
                else:
                    if(not pd.isna(entry_birth_year)):
                        if((entry_birth_year==vf_birth_year)):
                            ls.append(i[1])
                    else:
                        ls.append(i[1])

#instantiating output dataframe and merging with original data on saved index to get final table
  
df_last_name_address_match = (pd.DataFrame(ls, columns=df_address_match.columns)).reset_index()

df_last_name_address_match = df_last_name_address_match.merge(df_matching,left_on='entry_index',right_index=True)

df_final = df_matching.merge(df_last_name_address_match[['entry_index', 'vf_id']],left_index=True, right_on='entry_index').drop('entry_index',axis=1)
df_final = df_final.rename(columns={'vf_id':'matched_voterid'})
df_final.to_csv('matched.csv')



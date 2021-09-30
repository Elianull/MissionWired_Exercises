import numpy as np
import pandas as pd
import multiprocessing
import csv

files = ['cons.csv','cons_email.csv','cons_email_chapter_subscription.csv']

def exercise1(df):
    people = df.filter(['email','source','isunsub','create_dt_x','modified_dt'])
    people = people.rename(columns={'source':'code','isunsub':'is_unsub','create_dt_x':'created_dt','modified_dt':'update_dt'})
    print(people.columns)
    people.to_csv('people.csv',index=False)
    return people

def exercise2(people):
    acqDict = {}
    for index,row in people.iterrows():
        date = str(row['created_dt'])[:-9]
        if date in acqDict:
            acqDict[date] += 1
        else:
            acqDict[date] = 1
    #df = pd.DataFrame.from_dict(data=acqDict,orient="index",columns=['acquisition_date','acquisitions'])
    df = pd.DataFrame()
    df['acquisition_date']=acqDict.keys()
    df['acquisitions']=acqDict.values()
    df.to_csv('acquisition_facts.csv',index=False)


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=len(files))
    results = pool.map(pd.read_csv,files)

    df = results[0].merge(results[1],on='cons_id')
    df = df.merge(results[2],on='cons_email_id')
    print(df.columns)


    people = exercise1(df)
    exercise2(people)
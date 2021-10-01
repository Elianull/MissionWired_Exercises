import numpy as np
import pandas as pd
import multiprocessing

# Comment & uncomment if files are downloaded locally ahead of time.
files = ['https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv','https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv','https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv']
#files = ['cons.csv','cons_email.csv','cons_email_chapter_subscription.csv']

def exercise1(df):
    print("Producing people dataframe")
    people = df.filter(['email','source','isunsub','create_dt_x','modified_dt']) # Create new dataframe with required content
    people = people.rename(columns={'source':'code','isunsub':'is_unsub','create_dt_x':'created_dt','modified_dt':'update_dt'}) # Rename columns
    print("Exporting to people.csv")
    people.to_csv('people.csv',index=False) #Convert to CSV
    print("people.csv complete")
    return people

def exercise2(people):
    acqDict = {}
    print("Aggregating acquisition data")
    for index,row in people.iterrows(): #Iterate over dataframe, counting instances per date
        date = str(row['created_dt'])[:-9]
        if date in acqDict:
            acqDict[date] += 1
        else:
            acqDict[date] = 1
    df = pd.DataFrame() #Convert HashMap into DataFrame
    df['acquisition_date']=acqDict.keys()
    df['acquisitions']=acqDict.values()
    print("Exporting to acquisition_facts.csv")
    df.to_csv('acquisition_facts.csv',index=False) #Export DataFrame to CSV


if __name__ == "__main__":
    print("Multiprocessing data downloading and loading from:", files)
    pool = multiprocessing.Pool(processes=len(files)) #Create pool with n processes
    results = pool.map(pd.read_csv,files) #Collect from pool

    print("Merging datasets") #Join datasets
    df = results[0].merge(results[1],on='cons_id')
    df = df.merge(results[2],on='cons_email_id')

    people = exercise1(df)
    exercise2(people)
    print("Complete")
import pandas as pd

def uber_etl():

    #Extract: GCP cloud storage
    df=pd.read_csv('https://storage.googleapis.com/uber_data_kronos96/uber_data.csv')
    
    #Transformations#
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='%d/%m/%Y %H:%M')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format='%d/%m/%Y %H:%M')

    df['trip_distance'] = 1.60934*df['trip_distance'] #Miles to Kms
    df['trip_distance'] = df['trip_distance'].round(2)

    df = df[df['total_amount'] > 0] #Removing cancelled trips

    mapping = {1: "Credit Card", 2 : "Cash", 3 : "Credits", 4 :"Unknown"}
    df['payment_type'] = df['payment_type'].map(mapping)

    columns_to_drop = ['VendorID', 'RatecodeID','store_and_fwd_flag',] 
    df = df.drop(columns_to_drop, axis=1)

    new_columns = {'tpep_pickup_datetime': 'Pickup_Datetime', 'tpep_dropoff_datetime': 'Dropoff_Datetime','total_amount':'Final_total' }
    df = df.rename(columns=new_columns)

    df['Trip_Duration_mins'] = (df['Dropoff_Datetime'] - df['Pickup_Datetime']).dt.total_seconds()/ 60

    df = df[df['Trip_Duration_mins'] <=300] #Removing outliers

    df.insert(0, 'Tripid', df.reset_index().index + 1)
    
    #Load: AWS s3
    df.to_csv("s3://kronos-s3-bucket/uber_cleansed.csv")
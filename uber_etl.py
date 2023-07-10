import pandas as pd

def uber_etl():

    #Extract: GCP cloud storage
    df=pd.read_csv('https://storage.googleapis.com/uber_data_kronos96/uber_data.csv')

    #Transformations#
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='%d/%m/%Y %H:%M')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format='%d/%m/%Y %H:%M')

    df["Pickup_hour"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.hour
    conditions = [(df["Pickup_hour"] >= 0) & (df["Pickup_hour"] < 3),
        (df["Pickup_hour"] >= 3) & (df["Pickup_hour"] < 6),
        (df["Pickup_hour"] >= 6) & (df["Pickup_hour"] < 9),
        (df["Pickup_hour"] >= 9) & (df["Pickup_hour"] < 12),
        (df["Pickup_hour"] >= 12) & (df["Pickup_hour"] < 15)]
    choices = ["12am - 3am", "3am - 6am", "6am - 9am", "9am - 12pm", "12pm - 3pm"]
    df["Pickup_group"] = np.select(conditions, choices, default="other")              #Creating a grouping for pickup hour

    df['trip_distance'] = 1.60934*df['trip_distance'] #Miles to Kms
    df['trip_distance'] = df['trip_distance'].round(2)

    df = df[(df['pickup_longitude'] != 0) & (df['pickup_latitude'] != 0)]     #Removing incorrect coordinates
    df = df[(df['dropoff_longitude'] != 0) & (df['dropoff_latitude'] != 0)]   #Removing incorrect coordinates

    df = df[df['total_amount'] > 0]

    mapping = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Westchester", 5: "Negotiated fare", 6: "Group ride"}
    df['RatecodeID'] = df['RatecodeID'].map(mapping)

    mapping = {1: "Credit Card", 2 : "Cash", 3 : "Credits", 4 :"Unknown"}
    df['payment_type'] = df['payment_type'].map(mapping)

    mapping = {1: "Creative Mobile Tech", 2: "VeriFone Inc"}
    df['VendorID'] = df['VendorID'].map(mapping)

    columns_to_drop = ['store_and_fwd_flag']
    df = df.drop(columns_to_drop, axis=1)

    new_columns = {'tpep_pickup_datetime': 'Pickup_Datetime', 'tpep_dropoff_datetime': 'Dropoff_Datetime','total_amount':'Final_total' }
    df = df.rename(columns=new_columns)

    df['Trip_Duration_mins'] = (df['Dropoff_Datetime'] - df['Pickup_Datetime']).dt.total_seconds()/ 60

    df = df[df['Trip_Duration_mins'] <=300] #Removing outliers

    df.insert(0, 'Tripid', df.reset_index().index + 1)
    
    #Load: AWS s3
    df.to_csv("s3://kronos-s3-bucket/uber_cleansed.csv")
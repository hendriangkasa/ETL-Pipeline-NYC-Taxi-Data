import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    #create dim_datetime
    dim_datetime = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)

    dim_datetime['pick_hour'] = dim_datetime['tpep_pickup_datetime'].dt.hour
    dim_datetime['pick_day'] = dim_datetime['tpep_pickup_datetime'].dt.day
    dim_datetime['pick_month'] = dim_datetime['tpep_pickup_datetime'].dt.month
    dim_datetime['pick_year'] = dim_datetime['tpep_pickup_datetime'].dt.year
    dim_datetime['pick_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday

    dim_datetime['drop_hour'] = dim_datetime['tpep_dropoff_datetime'].dt.hour
    dim_datetime['drop_day'] = dim_datetime['tpep_dropoff_datetime'].dt.day
    dim_datetime['drop_month'] = dim_datetime['tpep_dropoff_datetime'].dt.month
    dim_datetime['drop_year'] = dim_datetime['tpep_dropoff_datetime'].dt.year
    dim_datetime['drop_weekday'] = dim_datetime['tpep_dropoff_datetime'].dt.weekday
    
    dim_datetime = dim_datetime.sort_values(by=['tpep_pickup_datetime','tpep_dropoff_datetime']).reset_index(drop=True)
    dim_datetime['datetime_id'] = dim_datetime.index
    dim_datetime = dim_datetime[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                            'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    
    # create dim_passenger_count
    dim_passenger_count = df[['passenger_count']].drop_duplicates().sort_values(by='passenger_count').reset_index(drop=True)
    dim_passenger_count['passenger_count_id'] = dim_passenger_count.index
    dim_passenger_count = dim_passenger_count[['passenger_count_id', 'passenger_count']]

    # create dim_trip_distance
    dim_trip_distance = df[['trip_distance']].drop_duplicates().sort_values(by='trip_distance').reset_index(drop=True)
    dim_trip_distance['trip_distance_id'] = dim_trip_distance.index
    dim_trip_distance = dim_trip_distance[['trip_distance_id', 'trip_distance']]
    
    # create dim_rate_code    
    dim_rate_code = df[['RatecodeID']].drop_duplicates().sort_values(by='RatecodeID').reset_index(drop=True)

    rate_code_type = {
        1: 'Standard rate',
        2: 'JFK',
        3: 'Newark',
        4: 'Nassau or Westchester',
        5: 'Negotiated fare',
        6: 'Group ride'
    }
    dim_rate_code['rate_code_name'] = dim_rate_code['RatecodeID'].map(rate_code_type)
    dim_rate_code['rate_code_id'] = dim_rate_code.index
    dim_rate_code = dim_rate_code[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    # create dim_payment_type
    dim_payment_type = df[['payment_type']].drop_duplicates().sort_values(by='payment_type').reset_index(drop=True)
    payment_type = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }

    dim_payment_type['payment_type_name'] = dim_payment_type['payment_type'].map(payment_type)
    dim_payment_type['payment_type_id'] = dim_payment_type.index
    dim_payment_type = dim_payment_type[['payment_type_id', 'payment_type', 'payment_type_name']]

    # create dim_pickup_location
    dim_pickup_location = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().sort_values(by=['pickup_latitude','pickup_longitude']).reset_index(drop=True)
    dim_pickup_location['pickup_location_id'] = dim_pickup_location.index
    dim_pickup_location = dim_pickup_location[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    # create dim_dropoff_location
    dim_dropoff_location = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().sort_values(by=['dropoff_latitude','dropoff_longitude']).reset_index(drop=True)
    dim_dropoff_location['dropoff_location_id'] = dim_dropoff_location.index
    dim_dropoff_location = dim_dropoff_location[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    # create dim_vendor
    dim_vendor = df[['VendorID']].drop_duplicates().reset_index(drop=True).sort_values(by='VendorID')

    vendor_type = {
        1: 'Creative Mobile Technologies, LLC',
        2: 'VeriFone Inc'
    }

    dim_vendor['vendor_name'] = dim_vendor['VendorID'].map(vendor_type)
    dim_vendor['vendor_id'] = dim_vendor.index
    dim_vendor = dim_vendor[['vendor_id', 'VendorID', 'vendor_name']]

    # create fact_table
    fact_table = df.merge(dim_passenger_count, left_on='passenger_count', right_on='passenger_count') \
             .merge(dim_trip_distance, left_on='trip_distance', right_on='trip_distance') \
             .merge(dim_rate_code, left_on='RatecodeID', right_on='RatecodeID') \
             .merge(dim_pickup_location, left_on=['pickup_latitude', 'pickup_longitude'], right_on=['pickup_latitude', 'pickup_longitude']) \
             .merge(dim_dropoff_location, left_on=['dropoff_latitude', 'dropoff_longitude'], right_on=['dropoff_latitude', 'dropoff_longitude'])\
             .merge(dim_datetime, left_on=['tpep_pickup_datetime','tpep_dropoff_datetime'], right_on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
             .merge(dim_payment_type, left_on='payment_type', right_on='payment_type') \
             .merge(dim_vendor, left_on='VendorID', right_on='VendorID') \
             [['trip_id','vendor_id', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']].drop_duplicates().sort_values(by='trip_id').reset_index(drop=True)

    return {"dim_datetime": dim_datetime.to_dict(orient="dict"),
        "dim_passenger_count": dim_passenger_count.to_dict(orient="dict"),
        "dim_payment_type": dim_payment_type.to_dict(orient="dict"),
        "dim_rate_code": dim_rate_code.to_dict(orient="dict"),
        "dim_trip_distance": dim_trip_distance.to_dict(orient="dict"),
        "dim_vendor": dim_vendor.to_dict(orient="dict"),
        "dim_pickup_location": dim_pickup_location.to_dict(orient="dict"),
        "dim_dropoff_location": dim_dropoff_location.to_dict(orient="dict"),
        "fact_table": fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

import os
from utils.timestream_handler import TimestreamHandler

AWS_ACCESS = os.environ['AWS_ACCESS']
AWS_SECRET = os.environ['AWS_SECRET']
AWS_REGION = os.environ['AWS_REGION_NAME']

tive_query = """
    WITH latest_recorded_time
     AS (SELECT t2.device_id,
                t2.current_location,
                Max(t2.time) AS last_scan_time
         FROM   "CCT_TRACKING_DB"."tive_events_logger" t2
         WHERE  ( t2.device_name = '{}'
                  AND t2.current_location != '' )
         GROUP  BY t2.device_id,
                   t2.current_location)
    SELECT t1.device_id,
       t1.entry_time_utc,
       t1.current_location,
       t1.latitude,
       t1.longitude,
       t1.device_name,
       t1.shipment_id,
       t1.battery_percentage,
       t1.temperature_celsius,
       t1.temperature_fahrenheit,
       t1.humidity_percentage
    FROM   latest_recorded_time t3
       INNER JOIN "CCT_TRACKING_DB"."tive_events_logger" t1
               ON t3.device_id = t1.device_id
                  AND t3.current_location = t1.current_location
                  AND t3.last_scan_time = t1.time
    WHERE  Date_format(t1.time, '%Y-%m-%dT%H:%i:%s') >= '{}'
       AND Date_format(t1.time, '%Y-%m-%dT%H:%i:%s') <= '{}'
    ORDER  BY t1.device_id,
          t1.entry_time_utc
    """

shipengine_query = """
    WITH latest_recorded_time AS (
        SELECT
            t2.tracking_number,
            t2.city_locality,
            MAX(t2.time) AS last_scan_time
        FROM
            CCT_TRACKING_DB.shipengine_events_logger t2
        WHERE
            t2.tracking_number IN {}
            AND t2.city_locality != ''
        GROUP BY
            t2.tracking_number,
            t2.city_locality
        )
    SELECT DISTINCT
        t1.tracking_number,
        t1.carrier_occurred_at,
        t1.city_locality,
        t1.latitude,
        t1.longitude,
        t1.status_code
    FROM
        latest_recorded_time t3
    INNER JOIN
        CCT_TRACKING_DB.shipengine_events_logger t1 ON t3.tracking_number = t1.tracking_number
                                                AND t3.city_locality = t1.city_locality
                                                AND t3.last_scan_time = t1.time
    ORDER BY
        t1.tracking_number,
        t1.carrier_occurred_at
    """
def get_shipment_details(event):

    platform = event['platform']
    
    tm_records = []
    
    # Initialize TimestreamHandler
    timestream_client = TimestreamHandler(
        aws_access = AWS_ACCESS,
        aws_secret = AWS_SECRET,
        aws_region = AWS_REGION)
    

    print("Establishing connection with AWS Timestream.")
    
    if platform == 'Tive' :
            
            # Retrieve tracking details from "CCT_TRACKING_DB"
            tm_records = timestream_client.select_records(query_string=tive_query.format(event['parameters']['device_id'],
                                                                                                 event['parameters']['start_time'],
                                                                                                  event['parameters']['stop_time']))
          

    elif platform == 'Shipengine' :
            
            # Retrieve tracking details from "CCT_TRACKING_DB"
            
            tm_records = timestream_client.select_records(query_string=shipengine_query.format(
                                                                                                 tuple(event['parameters']['tracking_numbers'])
                                                                                                ))

    return tm_records
            

    

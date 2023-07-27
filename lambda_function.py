import json
from metrics.timestream_data_tracking import get_shipment_details

def lambda_handler(event, context):
    
    response = get_shipment_details(event)
    # TODO implement
    return {
        'statusCode': 200,
        'body': response
    }

# event = {
#     "platform":"Shipengine",
#     "parameters":{
#         "tracking_numbers":["780737387827","1Z20V8R90601379833"]
#     }
# }

# context = None
# lambda_handler(event,context)
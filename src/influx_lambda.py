from datetime import datetime as dt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from schema import schema,tags
import json
import base64


bucket = "stock"
url = "ec2-18-222-89-133.us-east-2.compute.amazonaws.com:8086"
token = "PsPb_0Ke09TzAh47n1aCxcuazCUuvwbFWjIyV7ALB8voVkmCBkZUS3Vg34tV2NdJhRYwnTeONk_egrWGeMW9ig=="
org = "personal"

client = InfluxDBClient(url=url, token=token ,org=org)

write_api = client.write_api(write_options=SYNCHRONOUS)

def generate_influx_dict(record:dict,measurement,schema = schema,tags = tags):
    influx_fields = apply_schema(record,schema)
    extracted_tags = get_record_tags(record,tags)
    influx_dict = {
    "measurement": measurement,
    "tags": extracted_tags,
    "fields": influx_fields

    }
    return influx_dict

def apply_schema(record:dict,schema = schema):
    schema_keys = schema.keys()
    transformed_dict = dict()
    for key,value in record.items():
        if key in schema_keys:
            transformed_dict[key] = schema[key](value)
        else:
            transformed_dict[key] = value
    return transformed_dict

def get_record_tags(record,tags):
    extracted_tags = dict()
    for tag in tags:
        if tag in record.keys():
            extracted_tags[tag] = record[tag]
    return extracted_tags


def lambda_handler(event, context):
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       record = json.loads(payload)
       point = Point.from_dict(generate_influx_dict(record,"stocks"))
       write_api.write(bucket=bucket, record=point)
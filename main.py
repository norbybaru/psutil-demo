#!/usr/bin/env python3

from sys import argv

import boto3

import psutil
import time

from constants import TABLE_NAME, DB_NAME
from monitoring import Monitoring
from query import Query


def handler():

    device_name = argv[1]
    CURRENT_TIME = str(int(time.time() * 1000))

    monitoring = Monitoring()
    process = monitoring.get_running_process()

    dimensions = [ 
        {'Name': 'script', 'Value': process['name']},
        {'Name': 'device_id', 'Value': device_name},
        {'Name': 'user', 'Value': process['username']},
        {'Name': 'process_id', 'Value': str(process['pid'])}
    ]

    common_attributes = {
        'Dimensions': dimensions,
        'Time': CURRENT_TIME
    }
    
    record = {
        'cpu_usage': monitoring.get_cpu_usage_pct(), 
        'cpu_freq': monitoring.get_cpu_frequency(), 
        'cpu_temp': monitoring.get_cpu_temp(), 
        'ram_usage': int(monitoring.get_ram_usage() / 1024 / 1024), 
        'ram_total': int(monitoring.get_ram_total() / 1024 / 1024),
    }

    print("\n")
    # print(dimensions)
    # print(record)
    print("\n")

    # Output current CPU load as a percentage.
    print('System CPU load is {} %'.format(monitoring.get_cpu_usage_pct()))

    # Output current CPU frequency in MHz.
    print('CPU frequency is {} MHz'.format(monitoring.get_cpu_frequency()))

    # Output current CPU temperature in degrees Celsius
    print('CPU temperature is {} degC'.format(monitoring.get_cpu_temp()))

    # Output current RAM usage in MB
    print('RAM usage is {} MB'.format(int(monitoring.get_ram_usage() / 1024 / 1024)))

    # Output total RAM in MB
    print('RAM total is {} MB'.format(int(monitoring.get_ram_total() / 1024 / 1024)))

    # Output current RAM usage as a percentage.
    print('RAM usage is {} %'.format(monitoring.get_ram_usage_pct()))


    session = boto3.Session()
    write_client = session.client('timestream-write', region_name='eu-west-1')

    response = write_client.write_records(
        DatabaseName=DB_NAME, TableName=TABLE_NAME, Records=monitoring.get_write_records(), CommonAttributes=common_attributes
    )
    print(response)

    query_client = session.client('timestream-query', region_name='eu-west-1')
    query = Query(query_client)
    print("\n")
    query.run_query_with_multiple_pages(20)


        
if __name__ == "__main__":
    while True:
        handler()
        time.sleep(10)
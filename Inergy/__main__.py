import argparse
import logging
import os
import time

import happybase
from dotenv import load_dotenv
from neo4j import GraphDatabase

from Inergy.Entities import SensorEnum, RequestHourlyData
from Inergy.InergySource import InergySource
from constants import INVERTED_SENSOR_TYPE_TAXONOMY
from utils import create_supply, create_element, get_sensor_id, decode_hbase_values
from utils.neo4j import get_buildings, get_sensors, get_sensors_measurements


def insert_elements():
    logger.info("Starting elements integration")
    limit = 100
    skip = 0
    ttl = int(os.getenv('TTL'))
    t0 = time.time()
    while time.time() - t0 < ttl:
        with driver.session() as session:
            buildings = get_buildings(session, namespace=args.namespace, limit=limit, id_project=args.id_project,
                                      skip=limit * skip).data()
            logger.info(f"A subset-{skip} of {len(buildings)} elements has been started the integration.")
        to_insert = []
        for building in buildings:
            el = create_element(args, building)
            if el:
                to_insert.append(el.__dict__)

        InergySource.insert_elements(token=token['access_token'],
                                     data=to_insert)
        logger.info(f"The elements-{skip} has been integrated successfully.")
        if len(buildings) == limit:
            skip += 1
        else:
            break


def insert_supplies():
    limit = 100
    skip = 0
    ttl = int(os.getenv('TTL'))
    t0 = time.time()
    while time.time() - t0 < ttl:
        with driver.session() as session:
            sensors = get_sensors(session, id_project=args.id_project, namespace=args.namespace, limit=limit,
                                  skip=limit * skip).data()
            logger.info(f"A subset-{skip} of {len(sensors)} supplies has been started the integration.")

        to_insert = []
        for sensor in sensors:
            supply = create_supply(args, sensor)
            if supply:
                to_insert.append(supply.__dict__)
        res = InergySource.insert_supplies(token=token['access_token'], data=to_insert)
        logger.info(res)
        logger.info(f"The supplies-{skip} has been integrated successfully.")

        if len(sensors) == limit:
            skip += 1
        else:
            break


def clean_ts_data(data):
    pass


def insert_hourly_data():
    limit = 100
    skip = 0
    ttl = int(os.getenv('TTL'))
    t0 = time.time()
    while time.time() - t0 < ttl:
        with driver.session() as session:
            sensor_measure = get_sensors_measurements(session, namespace=args.namespace, limit=limit,
                                                      skip=limit * skip).data()
        for i in sensor_measure:
            _from, sensor_id, sensor_type = get_sensor_id(i['n'].get('uri'))
            if _from == 'CZ':
                cups = f"{sensor_id}-{sensor_type}"
            else:
                cups = sensor_id
            measure_id = i['m'].get('uri').split('#')[-1]

            req_hour_data = RequestHourlyData(instance=1, id_project=args.id_project, cups=cups,
                                              sensor=str(SensorEnum[sensor_type].value),
                                              hourly_data=[])

            get_data_hbase(measure_id, sensor_type)

            # if value['v:value'] != np.NaN and value['v:value'] != 'nan':
            #     hourly_data = HourlyData(value=float(value['v:value']), timestamp=timestamp.isoformat())
            # req_hour_data.hourly_data.append(hourly_data.__dict__)

            InergySource.update_hourly_data(token=token['access_token'], data=[req_hour_data.__dict__])

        if len(sensor_measure) == limit:
            skip += 1
        else:
            break


def get_data_hbase(measure_id, sensor_type):
    hbase_conn = happybase.Connection(host=os.getenv('HBASE_HOST'), port=int(os.getenv('HBASE_PORT')),
                                      table_prefix=os.getenv('HBASE_TABLE_PREFIX'),
                                      table_prefix_separator=os.getenv('HBASE_TABLE_PREFIX_SEPARATOR'))
    table = hbase_conn.table(
        os.getenv('HBASE_TABLE').format('online', INVERTED_SENSOR_TYPE_TAXONOMY.get(sensor_type)))
    ts_data = []
    # Gather TS data from measure_id
    for bucket in range(20):  # Bucket
        for key, value in table.scan(row_prefix='~'.join([str(float(bucket)), measure_id]).encode()):
            _, _, timestamp = key.decode().split('~')
            # timestamp = datetime.fromtimestamp(int(timestamp))
            value = decode_hbase_values(value=value)
            ts_data.append({'start': timestamp, 'end': value['info:end'], 'value': value['v:value']})

    return clean_ts_data(ts_data)


if __name__ == '__main__':
    # https://apiv20.inergy.online/docs/api.html#218--insertar-elementos
    # Load env. variables
    load_dotenv()

    # Set Logger
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO, filename='inergy.logs')

    logger = logging.getLogger('Inergy')

    # Set Arguments in CLI
    ap = argparse.ArgumentParser(description='Insert data to Inergy')
    ap.add_argument("--id_project", "-id_project", type=int, help="Project Id", required=True)
    ap.add_argument("--type", "-t", type=str, help="The user importing the data",
                    choices=['element', 'supplies', 'hourly_data', 'all'],
                    required=True)

    ap.add_argument("--namespace", "-n", required=True)
    args = ap.parse_args()

    # Get credentials
    token = InergySource.authenticate()
    logger.info("The authentication has been succeeded.")

    # Neo4J
    driver = GraphDatabase.driver(os.getenv('NEO4J_URI'),
                                  auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')))

    logger.info("The connection with database has been succeeded.")

    if args.type == 'element' or args.type == 'all':
        insert_elements()
        logger.info("The process of integrate elements has been completed.")

    if args.type == 'supplies' or args.type == 'all':
        insert_supplies()
        logger.info("The process of integrate supplies has been completed.")

    if args.type == 'hourly_data' or args.type == 'all':
        insert_hourly_data()

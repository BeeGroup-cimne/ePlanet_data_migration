import argparse
import os
import time

from dotenv import load_dotenv
from neo4j import GraphDatabase

from Inergy.Entities import Element, Location, Supply, SupplyEnum
from Inergy.InergySource import InergySource
from utils import create_supply, create_element, get_sensor_id
from utils.neo4j import get_buildings, get_sensors, get_sensors_measurements


def insert_elements():
    limit = 100
    skip = 0
    ttl = int(os.getenv('TTL'))
    t0 = time.time()
    while time.time() - t0 < ttl:
        with driver.session() as session:
            buildings = get_buildings(session, namespace=args.namespace, limit=limit,
                                      skip=limit * skip).data()
        to_insert = []
        for building in buildings:
            el = create_element(args, building)
            if el:
                to_insert.append(el.__dict__)

        InergySource.insert_elements(token=token['access_token'],
                                     data=to_insert)

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
            sensors = get_sensors(session, namespace=args.namespace, limit=limit,
                                  skip=limit * skip).data()

        to_insert = []
        for sensor in sensors:
            supply = create_supply(args, sensor)
            if supply:
                to_insert.append(supply.__dict__)

        InergySource.insert_supplies(token=token['access_token'], data=to_insert)

        if len(sensors) == limit:
            skip += 1
        else:
            break


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
                measure_id = i['m'].get('uri').split('#')[-1]

        if len(sensor_measure) == limit:
            skip += 1
        else:
            break


if __name__ == '__main__':
    load_dotenv()

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

    # Neo4J
    driver = GraphDatabase.driver(os.getenv('NEO4J_URI'),
                                  auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')))

    if args.type == 'element' or args.type == 'all':
        insert_elements()

    if args.type == 'supplies' or args.type == 'all':
        insert_supplies()

    if args.type == 'hourly_data' or args.type == 'all':
        insert_hourly_data()

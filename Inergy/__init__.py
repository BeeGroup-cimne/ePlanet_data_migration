import argparse
import os
import time
from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from neo4j import GraphDatabase

from Inergy.Entities import Element, Location, Supply, SupplyEnum
from Inergy.InergySource import InergySource
from utils.neo4j import get_buildings, get_sensors

SENSOR_TYPE_TAXONOMY = {"EnergyConsumptionGas": "GAS", "EnergyConsumptionWaterHeating": "WATER",
                        "EnergyConsumptionGridElectricity": "ELECTRICITY"}  # TODO: Add EnergyConsumptionDistrictHeating


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
            for i in buildings:
                building = i['n']
                location = i['l']

                loc = Location(
                    address=f"{location.get('bigg__addressStreetName')} {location.get('bigg__addressStreetNumber')}",
                    latitude=float(location.get('bigg__addressLatitude')[:-1]) if location.get(
                        'bigg__addressLatitude') else None,
                    longitude=float(location.get('bigg__addressLongitude')[:-1]) if location.get(
                        'bigg__addressLongitude') else None)

                if all(item in list(building.keys()) for item in
                       ['bigg__buildingIDFromOrganization', 'bigg__buildingName']):
                    el = Element(id_project=args.id_project,
                                 instance=1,
                                 code=building.get('bigg__buildingIDFromOrganization'),
                                 use='Equipment', typology=9, name=building.get('bigg__buildingName'),
                                 begin_date=str(date.today()),
                                 end_date=str(date.today() + relativedelta(years=10)),
                                 location=loc.__dict__)
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
            for sensor in sensors[:1]:
                split_uri = sensor['n']['uri'].split('-')

                code = None
                cups = None

                if len(split_uri) == 8:  # Czech (WATER,GAS,ELECTRICITY)
                    sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[5])

                    if not sensor_type:
                        continue

                    element_code = '-'.join(split_uri[2:5])
                    aux_val = f"{element_code}-{sensor_type}"
                    if sensor_type == 'WATER':
                        code = aux_val
                    else:
                        cups = aux_val

                else:  # Greece (ELECTRICITY)
                    sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[3])
                    element_code = cups = split_uri[2]
                supply = Supply(instance=1, id_project=args.id_project, code=code, cups=cups,
                                id_source=SupplyEnum[sensor_type].value, element_code=element_code,
                                use='Equipment',
                                id_zone=1,
                                begin_date=str(sensor['n']['bigg__timeSeriesStart'].date()),
                                end_date=str(sensor['n']['bigg__timeSeriesEnd'].date()))
                to_insert.append(supply.__dict__)
            InergySource.insert_supplies(token=token['access_token'], data=to_insert)

            if len(sensors) == limit:
                skip += 1
            else:
                break


if __name__ == '__main__':
    load_dotenv()

    # Set Arguments in CLI
    ap = argparse.ArgumentParser(description='Insert data to Inergy')
    ap.add_argument("--id_project", "-id_project", help="Project Id", required=True)
    ap.add_argument("--type", "-t", help="The user importing the data", choices=['element', 'supplies', 'all'],
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

    # "Suministros"
    if args.type == 'supplies' or args.type == 'all':
        insert_supplies()

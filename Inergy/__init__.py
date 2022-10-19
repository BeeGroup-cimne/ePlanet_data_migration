import argparse
import os
import time
from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from neo4j import GraphDatabase

from Inergy.Entities import Element, Location
from Inergy.InergySource import InergySource
from utils.neo4j import get_buildings

if __name__ == '__main__':
    load_dotenv()

    # Set Arguments in CLI
    ap = argparse.ArgumentParser(description='Insert data to Inergy')
    ap.add_argument("--id_project", "-id_project", help="Project Id", required=True)
    ap.add_argument("--type", "-t", help="The user importing the data", choices=['element', 'all'], required=True)
    ap.add_argument("--namespace", "-n", required=True)
    args = ap.parse_args()

    # Get credentials
    token = InergySource.authenticate()

    # Neo4J
    driver = GraphDatabase.driver(os.getenv('NEO4J_URI'),
                                  auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')))

    if args.type == 'element' or args.type == 'all':
        limit = 100
        skip = 0
        ttl = int(os.getenv('TTL'))
        t0 = time.time()

        while time.time() - t0 < ttl:
            with driver.session() as session:
                # buildings = get_buildings(session, namespace=args.namespace, limit=limit, skip=limit * skip).data()
                buildings = get_buildings(session, namespace='https://eplanet.eu#BUILDING-', limit=limit,
                                          skip=limit * skip).data()
                to_insert = []
                for i in buildings[:1]:
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
                        el = Element(id_project=852,  # args.id_project,
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

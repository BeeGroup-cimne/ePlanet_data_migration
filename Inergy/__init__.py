import argparse
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

from Inergy.InergySource import InergySource
from utils import create_element
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

        with driver.session() as session:
            print(get_buildings(session).data())
        InergySource.insert_elements(token=token['access_token'], data={})

        data = create_element()

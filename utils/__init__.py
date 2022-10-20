from datetime import date

from dateutil.relativedelta import relativedelta

from Inergy import SupplyEnum, Supply, Location, Element
from utils.constants import SENSOR_TYPE_TAXONOMY


def create_supply(args, sensor):
    split_uri = sensor['n']['uri'].split('-')

    code = None
    cups = None

    if len(split_uri) == 8:  # Czech (WATER,GAS,ELECTRICITY)
        sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[5])

        if not sensor_type:
            return None

        element_code = '-'.join(split_uri[2:5])
        aux_val = f"{element_code}-{sensor_type}"
        if sensor_type == 'WATER':
            code = aux_val
        else:
            cups = aux_val

    else:  # Greece (ELECTRICITY)
        sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[3])
        element_code = cups = split_uri[2]

    return Supply(instance=1, id_project=args.id_project, code=code, cups=cups,
                  id_source=SupplyEnum[sensor_type].value, element_code=element_code,
                  use='Equipment',
                  id_zone=1,
                  begin_date=str(sensor['n']['bigg__timeSeriesStart'].date()),
                  end_date=str(sensor['n']['bigg__timeSeriesEnd'].date()))


def create_element(args, i):
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

        return el
    return None

from datetime import date

from dateutil.relativedelta import relativedelta

from Inergy.Entities import Element, Location, SupplyEnum, Supply
from constants import SENSOR_TYPE_TAXONOMY


def create_supply(args, sensor):
    _from, sensor_id, sensor_type = get_sensor_id(sensor['s']['uri'])

    if not sensor_type:
        return None

    cups = f"{sensor_id}-{sensor_type}" if _from == 'CZ' else sensor_id
    code = f'{cups}-CODE'

    return Supply(instance=1, id_project=args.id_project, code=code, cups=cups,
                  id_source=SupplyEnum[sensor_type].value, element_code=sensor_id,
                  use='Equipment',
                  id_zone=1,
                  begin_date=str(sensor['s']['bigg__timeSeriesStart'].date()),
                  end_date=str(sensor['s']['bigg__timeSeriesEnd'].date()))


def create_location(location, city):
    address = None
    address_street = location.get('bigg__addressStreetName')
    address_number = location.get('bigg__addressStreetNumber')

    if address_street:
        address = address_street
        if address_street:
            address += f' {address_number}'

    if location.get('bigg__addressLatitude') and location.get('bigg__addressLatitude'):
        latitude = float(location.get('bigg__addressLatitude')[:-1])
        longitude = float(location.get('bigg__addressLongitude')[:-1])
    else:
        latitude = float(city.get('wgs__lat'))
        longitude = float(city.get('wgs__long'))

    return Location(address=address, latitude=latitude, longitude=longitude)


def create_element(args, i):
    building = i['n']
    location = i['l']
    city = i['c']

    loc = create_location(location, city)

    if all(item in list(building.keys()) for item in
           ['bigg__buildingIDFromOrganization', 'bigg__buildingName']):
        el = Element(id_project=args.id_project,
                     instance=1,
                     code=str(building.get('bigg__buildingIDFromOrganization')),
                     use='Equipment', typology=9, name=building.get('bigg__buildingName'),
                     begin_date=str(date.today()),
                     end_date=str(date.today() + relativedelta(years=10)),
                     location=loc.__dict__)

        return el
    return None


def get_sensor_id(sensor_uri):
    split_uri = sensor_uri.split('-')
    if len(split_uri) == 8:
        sensor_id = '-'.join(split_uri[2:5])
        sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[5])
        _from = 'CZ'

    else:
        sensor_id = split_uri[2]
        sensor_type = SENSOR_TYPE_TAXONOMY.get(split_uri[3])
        _from = 'GR'

    return _from, sensor_id, sensor_type


def decode_hbase_values(value):
    item = dict()

    for k, v in value.items():
        item.update({k.decode(): v.decode()})
    return item

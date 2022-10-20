def get_buildings(session, namespace, skip=0, limit=100):
    query = f"""MATCH (n:bigg__Building)-[r:bigg__hasLocationInfo]-(l:bigg__LocationInfo)
                WHERE n.uri contains '{namespace}'
                RETURN n,l
                SKIP {skip}
                LIMIT {limit}"""
    return session.run(query)


def get_point_of_delivery(session, namespace, skip, limit):
    query = f"""MATCH (n:bigg__UtilityPointOfDelivery)-[r:bigg__hasUtilityType]-(u)
                WHERE n.uri contains "{namespace}"
                RETURN n.uri ,u.rdfs__label
                SKIP {skip}
                LIMIT {limit}
                """
    return session.run(query)


def get_sensors(session, namespace, skip, limit):
    query = f"""MATCH (n:bigg__Sensor)-[r:bigg__hasMeasuredProperty]-(o)
                WHERE n.uri contains '{namespace}'
                RETURN n, o.uri
                SKIP {skip}
                LIMIT {limit}
                """
    return session.run(query)

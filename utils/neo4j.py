def get_buildings(session, namespace, skip=0, limit=100):
    query = f"MATCH (n:bigg__Building) WHERE n.uri contains 'eplanet' return n skip {skip} limit {limit}"
    return session.run(query)

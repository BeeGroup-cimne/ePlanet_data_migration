def get_buildings(session, skip=0, limit=100):
    query = f"MATCH (n:bigg__Building) WHERE n.uri contains 'eplanet' return n"
    return session.run(query)

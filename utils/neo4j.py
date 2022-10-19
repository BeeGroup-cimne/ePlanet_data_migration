def get_buildings(session, namespace, skip=0, limit=100):
    query = f"""MATCH (n:bigg__Building)-[r:bigg__hasLocationInfo]-(l:bigg__LocationInfo)
                WHERE n.uri contains '{namespace}'
                RETURN n,l
                SKIP {skip}
                LIMIT {limit}"""
    return session.run(query)

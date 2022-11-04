# ePlanet Data Migrations

## Run script

    python3 -m Inergy --id_project <id_project> --type ['element', 'supplies', 'hourly_data', 'all'] --namespace <namespace>

## Environment Variables

    INERGY_USERNAME=
    INERGY_PASSWORD=
    INERGY_BASE_URL=
    
    NEO4J_URI=
    NEO4J_USERNAME=
    NEO4J_PASSWORD=
    
    TTL=
    
    HBASE_HOST=
    HBASE_PORT=
    HBASE_TABLE_PREFIX=
    HBASE_TABLE_PREFIX_SEPARATOR=
    HBASE_TABLE_GR=harmonized_{0}_{1}_100_SUM__eplanet
    HBASE_TABLE_CZ=harmonized_{0}_{1}_000_SUM_PT1M_eplanet
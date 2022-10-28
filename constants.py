SENSOR_TYPE_TAXONOMY = {"EnergyConsumptionGas": "GAS", "EnergyConsumptionWaterHeating": "WATER",
                        "EnergyConsumptionGridElectricity": "ELECTRICITY"}  # TODO: Add EnergyConsumptionDistrictHeating

INVERTED_SENSOR_TYPE_TAXONOMY = {v: k for k, v in SENSOR_TYPE_TAXONOMY.items()}

PROJECTS = {852: 'Agios Nikolaos', 853: 'Rethymno', 856: 'Karolinka', 857: 'Racková'}

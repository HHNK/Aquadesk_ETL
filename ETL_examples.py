import yaml
import pandas as pd
from functions.ETL_ecosys import ecosys_dataparser

"""
'''
    File name: ETL_ecosys.py
    Author: Jeroen Baars (j.baars@hhnk.nl)
    Date created: 12/04/2021
    Date last modified: 02/06/2021
    Python Version: 3.8.6
'''
Download macro invertebrates or (physical)-chemical data from ecosys
"""

# API keys
config_keys = yaml.safe_load(open("api_keys.yml"))
api_key = config_keys["hhnk"]

# Example code for downloading all HHNK biological MACEV data with all possible columns
etl_ecosys = ecosys_dataparser("https://ddeco-preview.aquadesk.nl/v2/")

#Query filters for macrofauna, all chemical measurements or all quantity (PH, etc..) measurements
query={'macev':"taxontype:eq:'MACEV'",
        'chem':"parametertype:eq:'CHEMS'",
        'fys_chem':"measurementpackage:eq:'MO.KG'"}

# Get example query (no dataframe) macro invertebrates
sample_query = etl_ecosys.return_query(query_url="measurements",
                                        query_filter = query['macev'],
                                        api_key=api_key)
# print(sample_query)

# Get dataframe all measurements macro invertebrates
data_macev = etl_ecosys.parse_data_dump(query_url="measurements",
                                        query_filter = query['macev'],
                                        api_key=api_key)
data_macev.to_csv("data_macev.csv", index=False)


# Get dataframe all measurements chemical measurements
data_chem = etl_ecosys.parse_data_dump(query_url="measurements",
                                        query_filter = query['chem'],
                                        api_key=api_key)
data_chem.to_csv("data_chem.csv", index=False)


# Get dataframe all measurements fysisch chemisch (PH/ZICHT/..)
data_fyschem = etl_ecosys.parse_data_dump(query_url="measurements",
                                        query_filter = query['fys_chem'],
                                        api_key=api_key)
data_fyschem.to_csv("data_fyschem.csv", index=False)


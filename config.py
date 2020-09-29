from collections import namedtuple
import configparser

DWHCfg = namedtuple(
        'DWHCfg',
        [
            'ACCESS_KEY',
            'SECRET_KEY',
            'CLUSTER_TYPE',
            'NUM_NODES',
            'NODE_TYPE',
            'CLUSTER_IDENTIFIER',
            'DB',
            'DB_USER',
            'DB_PASSWORD',
            'PORT',
            'IAM_ROLE_NAME'
        ])

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

_dwh_cfg = DWHCfg(ACCESS_KEY = config.get('AWS','KEY'),
                  SECRET_KEY=config.get('AWS','SECRET'),
                  CLUSTER_TYPE=config.get("DWH", "DWH_CLUSTER_TYPE"),
                  NUM_NODES=config.get("DWH", "DWH_NUM_NODES"),
                  NODE_TYPE=config.get("DWH", "DWH_NODE_TYPE"),
                  CLUSTER_IDENTIFIER=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
                  DB=config.get("DWH", "DWH_DB"),
                  DB_USER=config.get("DWH", "DWH_DB_USER"),
                  DB_PASSWORD=config.get("DWH", "DWH_DB_PASSWORD"),
                  PORT=config.get("DWH", "DWH_PORT"),
                  IAM_ROLE_NAME=config.get("DWH", "DWH_IAM_ROLE_NAME"))

def get_config():
    """
    Get configuration variables from the data warehouse config file.
    This function assumes that there is a file named dwh.cfg in the current
    working directory.
    The config file is expected to have the following
    "sections" constaining the listed "keys".
    Section:    [AWS]
        Keys:       KEY
                    SECRET

    Section:    [DWH]
        Keys:       DWH_CLUSTER_TYPE
                    DWH_NUM_NODES
                    DWH_NODE_TYPE
                    DWH_IAM_ROLE_NAME
                    DWH_CLUSTER_IDENTIFIER
                    DWH_DB
                    DWH_DB_USER
                    DWH_DB_PASSWORD
                    DWH_PORT
    """
    return _dwh_cfg

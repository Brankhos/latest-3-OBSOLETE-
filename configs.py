from datetime import datetime

database_name = "coins_"
delete_database = False
periods = ["4h", "15m", "1w"]

db_pool_att = {'pool_name': 'pynative_pool',
               'pool_reset_session': True,
               'user': 'root',
               'password': '',
               'host': 'localhost',
               'raise_on_warnings': True
               }

db_att = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'raise_on_warnings': True
}

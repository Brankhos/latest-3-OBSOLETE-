import numpy as np
import requests
from request_weight import request_weight
import mysql.connector
from mysql.connector import errorcode
import configs


class get_list:

    def __init__(self, wei_check, cnx):
        self.cnx = cnx
        self.coin_list = np.array([])
        self.periods = configs.periods
        self.coin_infos = {}
        self.block_list = np.array([])
        self.white_list = np.array([])
        self.symbol_att = {"over": "False", "under": "False"}
        self.wei_check = wei_check
        self.req_header = {
            "Connection": "close"
        }
        datas = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=self.req_header)
        #datas = requests.get("http://localhost/tt.txt", headers=self.req_header)

        try:
            self.weight = int(datas.headers['x-mbx-used-weight-1m'])
        except:
            self.weight = 0
        datas = datas.json()
        """
        with open('futures.json', 'w') as f:
            json.dump(datas, f)
        """
        symbol_infos = datas["symbols"]
        serv = requests.get("https://fapi.binance.com/fapi/v1/time", headers=self.req_header)
        try:
            self.weight += int(serv.headers['x-mbx-used-weight-1m'])
        except:
            self.weight = 0
        serv = serv.json()
        self.server_time = serv["serverTime"]
        for symbol_info in symbol_infos:
            cond1 = symbol_info["quoteAsset"] == "USDT"
            cond2 = symbol_info["status"] == "TRADING"
            cond3 = symbol_info["contractType"] == "PERPETUAL"
            cond4 = not np.isin(symbol_info["baseAsset"], self.block_list) or np.isin(symbol_info["baseAsset"],
                                                                                      self.white_list)

            if cond1 and cond2 and cond3 and cond4:
                self.coin_list = np.append(self.coin_list, symbol_info["symbol"])
                self.coin_infos[symbol_info["symbol"]] = {}
                for period in self.periods:
                    self.coin_infos[symbol_info["symbol"]][period] = self.symbol_att

        """
        self.coin_infos = {}
        self.coin_list = np.array([])

        self.coin_infos["TEST2"] = {'5m': {'over': 'False', 'under': 'False'}, '15m': {'over': 'False', 'under': 'False'},
                    '1h': {'over': 'False', 'under': 'False'}, '4h': {'over': 'False', 'under': 'False'}}
        self.coin_list = np.append(self.coin_list, "TEST2")
        """
        self.wei_check.set_weight(self.weight)

    def update(self):
        datas = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=self.req_header)
        #datas = requests.get("http://localhost/tt.txt", headers=self.req_header)
        try:
            self.weight = int(datas.headers['x-mbx-used-weight-1m'])
        except:
            self.weight = 0
        datas = datas.json()
        """
        with open('futures.json', 'w') as f:
            json.dump(datas, f)
        """
        symbol_infos = datas["symbols"]
        serv = requests.get("https://fapi.binance.com/fapi/v1/time", headers=self.req_header)

        try:
            self.weight += int(serv.headers['x-mbx-used-weight-1m'])
        except:
            self.weight = 0
        serv = serv.json()
        self.server_time = serv["serverTime"]
        updated_list = np.array([])
        for symbol_info in symbol_infos:
            cond1 = symbol_info["quoteAsset"] == "USDT"
            cond2 = symbol_info["status"] == "TRADING"
            cond3 = symbol_info["contractType"] == "PERPETUAL"
            cond4 = not np.isin(symbol_info["baseAsset"], self.block_list) or np.isin(symbol_info["baseAsset"],
                                                                                      self.white_list)
            cond5 = symbol_info['symbol'] not in self.coin_list
            if cond1 and cond2 and cond3 and cond4:
                updated_list = np.append(updated_list, [symbol_info["symbol"]])
                if cond5:
                    self.coin_list = np.append(self.coin_list, symbol_info["symbol"])
                    self.coin_infos[symbol_info["symbol"]] = {}
                    for period in self.periods:
                        self.coin_infos[symbol_info["symbol"]][period] = self.symbol_att

        for updated_coin in self.coin_list:
            if updated_coin not in updated_list:
                cursor = self.cnx.cursor()
                xec = "DROP TABLE {}".format(updated_coin)

                try:
                    print("Coin silindi: {}".format(updated_coin))
                    cursor.execute(xec)
                except mysql.connector.Error as err:
                    print("Kaldırılacak tablo yok.\n", err)
                else:
                    cursor.close()

                self.coin_list = [x for x in self.coin_list if updated_coin not in x]
                del self.coin_infos[updated_coin]
        # self.coin_list = updated_list
        self.wei_check.set_weight(self.weight)


if "__main__" == __name__:
    config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'raise_on_warnings': True
    }

    try:
        cnx = mysql.connector.connect(**config)
        print("Veritabanına bağlantı başarılı")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("", err)
    cnx.autocommit = True
    cursor = cnx.cursor()
    try:
        cursor.execute("USE {}".format(configs.database_name))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(configs.database_name))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(configs.database_name))
            cnx.database = configs.database_name
        else:
            print(err)
            exit(1)

    reqwei = request_weight()
    gl = get_list(reqwei, cnx)
    gl.update()
    liste = gl.coin_list

    print(reqwei.get_weight())

    print(len(liste))
    """
    print(liste)

    for i in liste:
        print(i)
    """
    """
    liste = gl.coin_infos
    print(len(liste))
    print(liste)
    for i in liste:
        print(i)
    """

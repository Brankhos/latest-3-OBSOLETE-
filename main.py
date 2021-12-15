from get_list import get_list
from mysql.connector import pooling
import mysql.connector
import configs
import multiprocessing
from mysql.connector import errorcode
from request_weight import request_weight
from database_setup import setup_cnx
import numpy as np
import datetime
import threading
import time
import asyncio
import aiohttp
import sys
import atexit
import requests
import json

if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def reset_wei(weight):
    weight.reset_weight()

async def coin_calculator(symbol_name, cursor, weight,  session):
    try:
        add_coindata = (
            "INSERT INTO {} (open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_it) "
            "VALUES ".format(symbol_name))
        for period in configs.periods:

            sp = int(period[:-1])

            sp2 = period[-1]

            if sp2 == "m":
                cc = 60
            elif sp2 == "h":
                cc = 3600
            elif sp2 == "d":
                cc = 86400
            elif sp2 == "w":
                cc = 604800
            time_now = int(datetime.datetime.now().timestamp())
            try:
                cursor.execute("USE {}{}".format(configs.database_name, period))
                cursor.execute("SELECT `open_time` FROM `" + symbol_name +
                               "` ORDER BY `open_time` DESC LIMIT 1")
                taked_rec = cursor.fetchall()[0][0]
                cursor.execute(("DELETE FROM `{}` WHERE `open_time` = '{}'".format(symbol_name, taked_rec)))
                date = taked_rec
            except:
                print("Muhtemel yeni coin. Veri mevcut değil: {}".format(symbol_name))
                date = ((time_now - (time_now % (sp * cc))) - (sp * cc * 995)) * 1000
                # print(datetime.datetime.fromtimestamp(int(date/1000)))

            start_time = (time_now - (sp * cc * 95)) * 1000

            if date - start_time < 0:
                remaining_limit = 1000
                remaining_weight = 5
            else:
                remaining_limit = 99
                remaining_weight = 1

            sayac = 1

            while True:
                if weight.check_weight():
                    weight.add_weight(remaining_weight)
                    url = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol_name}&interval={period}&limit={remaining_limit}&startTime={date}'

                    try:
                        async with session.get(url) as resp:
                            backup = resp
                            weight.check_server_wei(int(resp.headers['x-mbx-used-weight-1m']))

                            klines = await resp.json()

                            klines = np.array(klines)
                            klines = "(" + "),(".join(",".join(x for x in y) for y in klines) + ")"
                            add_coindata_t = add_coindata + klines
                            cursor.execute("USE {}{}".format(configs.database_name, period))
                            cursor.execute(add_coindata_t)
                        break
                    except aiohttp.ClientConnectorError as e:
                        print('Connection Error', str(e))

                    except Exception as E:
                        print("Session Hata oluştu: ", E)
                        print(backup.headers)
                        print(backup)
                        time.sleep(sayac)
                        sayac += 5

                else:
                    time.sleep(1)
    except (SystemExit, KeyboardInterrupt):
        pass



async def pre_calculator(array,weight,cursor):
    tasks = []
    connector = aiohttp.TCPConnector(limit=50)
    session = aiohttp.ClientSession(connector=connector, trust_env=True)

    try:
        for symbol_name in array:
            tasks.append(asyncio.create_task(coin_calculator(symbol_name, cursor, weight,  session)))
        await asyncio.gather(*tasks)

    except Exception as E:
        print("Session hata sonucu kapatıldı", E)
        await session.close()
    else:
        await session.close()


def prepare_coin(array, weight, periods,number, cnx):
    try:
        cnx = setup_cnx(pri=False)
        cursor = cnx.cursor()
        for period in periods:
            cursor.execute("USE {}{}".format(configs.database_name,period))
            for symbol_name in array:
                xec = "CREATE TABLE `" + symbol_name + "` (" \
                                                       "  `open_time` bigint NOT NULL primary key," \
                                                       "  `open` FLOAT NOT NULL," \
                                                       "  `high` FLOAT NOT NULL," \
                                                       "  `low` FLOAT NOT NULL," \
                                                       "  `close` FLOAT NOT NULL," \
                                                       "  `volume` FLOAT NOT NULL," \
                                                       "  `close_time` bigint NOT NULL," \
                                                       "  `quote_asset_volume` FLOAT NOT NULL," \
                                                       "  `number_of_trades` int(11) NOT NULL," \
                                                       "  `taker_buy_base_asset_volume` FLOAT NOT NULL," \
                                                       "  `taker_buy_quote_asset_volume` FLOAT NOT NULL," \
                                                       "  `ignore_it` FLOAT NOT NULL" \
                                                       ") ENGINE=InnoDB"
                try:
                    cursor.execute(xec)
                except mysql.connector.Error:
                    pass
        asyncio.run(pre_calculator(array, weight, cursor))

    finally:
        cnx.close()
        cursor.close()


def main():
    cpu_count = multiprocessing.cpu_count()
    if cpu_count > 8:
        cpu_count = 8
    cnx = setup_cnx(configs.delete_database)
    cursor = cnx.cursor()
    try:
        weight = request_weight()
        server_datas = get_list(weight, cnx)
        server_time = server_datas.server_time
        # wait_time = 61 - (int(server_time / 1000) % 60)
        wait_time = 1
        print(wait_time, "başlangıç")
        time.sleep(wait_time)
        res_wei_task = threading.Thread(target=reset_wei, args=(weight,), daemon=True)
        res_wei_task.start()
        while True:
            server_datas.update()
            coin_list = server_datas.coin_list
            periods = server_datas.periods

            per_cpu = int(len(coin_list) / cpu_count)
            cout = len(coin_list) % cpu_count
            print(len(coin_list))
            print(cpu_count)
            task = []

            for period in periods:

                def create_database(cursor):
                    try:
                        cursor.execute(
                            "CREATE DATABASE {}{} DEFAULT CHARACTER SET 'utf8'".format(configs.database_name,period))
                    except mysql.connector.Error as err:
                        print("FUTURES: Failed creating database: {}".format(err))
                        exit(1)

                try:
                    cursor.execute("USE {}{}".format(configs.database_name,period))
                except mysql.connector.Error as err:
                    print("Database {} does not exists.".format(period), end=" ")
                    if err.errno == errorcode.ER_BAD_DB_ERROR:
                        create_database(cursor)
                        print("Created successfully.")
                    else:
                        print(err)
                        exit(1)



            for i in range(cpu_count):
                if i == cpu_count - 1:
                    add_ex = cout
                else:
                    add_ex = 0
                #print(coin_list[i*per_cpu:(i+1)*per_cpu+add_ex])
                t = threading.Thread(target=prepare_coin, args=(coin_list[i*per_cpu:(i+1)*per_cpu+add_ex], weight, periods,i,cnx,), daemon=True)
                task.append(t)
            for i in task:
                i.start()
            for i in task:
                i.join()

            local_data = json.load("version.txt")
            git_data = requests.get(
                "https://Brankhos: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@raw.githubusercontent.com/Brankhos/latest-3/main/version.txt").json()
            if local_data["Version"] < git_data["Version"]:
                print("Günvelleme uygulanıyor")
                sys.exit()
            server_time = server_datas.server_time
            # wait_time = 61 - (int(server_time / 1000) % 60)
            wait_time = 20
            print(wait_time, "Bitiş tekrar")
            time.sleep(wait_time)

            # break ############

    except (SystemExit, KeyboardInterrupt):
        print("---------------\nSistemden çıkış yapılıyor...\n---------------")

    except Exception as E:
        print("---------------\nBir sorun oluştu\nÇıkış yapılıyor\n---------------", E)
    else:
        print("---------------\nKomutlar tamamlandı\n---------------")
    finally:
        if cnx.is_connected():
            cursor.close()
            print("Main Cursor kapatıldı")
            cnx.close()
            print("Main CNX kapatıldı")

        else:
            print("Main CNX zaten kapalı")
        weight.close_weight()
        print("Weight kapatıldı")



if __name__ == "__main__":
    main()
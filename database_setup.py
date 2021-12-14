from mysql.connector import pooling
import configs
from mysql.connector import errorcode
import mysql.connector


def setup_cnx(delete_database=False,pri = True, cpu_count = 4, pooll = False):
    if pooll == True:
        cpu_count += 1
        config = configs.db_pool_att
        config["pool_size"] = cpu_count
    else:
        config = configs.db_att

    try:
        if pooll == True:
            cnx = pooling.MySQLConnectionPool(**config)
        else:
            cnx = mysql.connector.connect(**config)
        if pri:
            print("Veritabanına bağlantı başarılı")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("", err)
    cnx.autocommit = True

    if pooll == True:

        connection_object = cnx.get_connection()

        cursor = connection_object.cursor()
    else:
        cursor = cnx.cursor()
    if delete_database:
        for period in configs.periods:
            try:
                cursor.execute("DROP DATABASE `coins_{}`".format(period))  # Databaseyi silmek için
                if pri:
                    print("Veritabanı silindi {}".format(period))
            except mysql.connector.Error as err:
                print("Kaldırılacak veribabanı yok.\n", err)

    if pooll == True:
        if connection_object.is_connected():
            cursor.close()
            connection_object.close()
    else:
        cursor.close()

    return cnx

import pymysql as pm
import pandas as pd
import csv
import itertools
import sys


try:
    #connect to mysql platform
    conn = pm.connect(host='localhost', user='test', password="")

    #creating cursor for executing sql
    cur=conn.cursor()
# running a query to return only the first 100 lines of the data by using "SELECT * FROM function"
    sql = "SELECT * FROM `pollution-db2`.`reading` LIMIT 100;"
    # cursor executing an sql string
    cur.execute(sql)
    # An index list(array) will be gotten as the result
    result = cur.fetchall()
    # using an insert statements for the creation of an insert-100.sql file
    data_file = open("insert-100.sql", "w")

    # generation of an SQL insert statement by using  loop function through the result

    for sql_readings in result:
        sql_insert = """
             INSERT INTO `pollution-db2`.`reading`
              (nox, no2, locationid, datetime, pm10,nvpm10, vpm10, nvpm25, vpm25, co, o3,so2,
               temperature, rh, airpressure, datestart,date_end, `current`, instrument_type,pm25, `no`) 
               VALUES({},{},{},'{}',{},{},{},{},{},{},{},{},{},{},{},'{}','{}',{},'{}',{}, `{}`)

                """.format(
            sql_readings[1],
            sql_readings[2],
            sql_readings[3],
            sql_readings[4],
            sql_readings[5],
            sql_readings[6],
            sql_readings[7],
            sql_readings[8],
            sql_readings[9],
            sql_readings[10],
            sql_readings[11],
            sql_readings[12],
            sql_readings[13],
            sql_readings[14],
            sql_readings[15],
            sql_readings[16],
            sql_readings[17],
            sql_readings[18],
            sql_readings[19],
            bool(sql_readings[20]),
            sql_readings[10],
            sql_readings[21])

        data_file.write(sql_insert)
    data_file.close()
    conn.close()


except BaseException as err:

    print(f"An error occured: {err}")
    sys.exit(1)

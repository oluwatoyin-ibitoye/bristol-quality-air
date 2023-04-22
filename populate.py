# module imports
import mariadb
import mysql.connector
import sys
import csv
import itertools

try:
    # set the user and passoword
    # connect to mariaDB platform
    conn = mariadb.connect(
        user="test",
        password="",
        host="127.0.0.1",  # localhost will also do
        port=3306,  # possibly some other port
    )

    # make and get the cursor
    cur = conn.cursor()

    # execute cursor with query (using heredoc multi_line string)
    # cur.execute("SHOW DATABASES")

    sql = "DROP SCHEMA IF EXISTS `pollution-db2`;"
    cur.execute(sql)
    conn.commit()

    # creating database instance

    create_database = "CREATE SCHEMA IF NOT EXISTS `pollution-db2`;"
    cur.execute(create_database)
    conn.commit()
    locationentity_table_sql = """

            CREATE TABLE IF NOT EXISTS `pollution-db2`.`locationentity`
              (
          `idlocation_entity` INT NOT NULL,
          `geo_point_2d` VARCHAR(30) NOT NULL,
          `name` VARCHAR(35) NOT NULL,
          PRIMARY KEY (`idlocation_entity`)
        )
               ENGINE = InnoDB;

    """

    # craeting a table
    cur.execute(locationentity_table_sql)
    conn.commit()

    conn.commit()
    reading = """


              CREATE TABLE IF NOT EXISTS `pollution-db2`.`reading` 
              (
              `id` INT NOT NULL AUTO_INCREMENT,
              `nox` FLOAT(10,6) NULL,
              `no2` FLOAT(10,6) NULL,
              `locationid` INT NOT NULL,
              `datetime` VARCHAR(200) NULL,
              `pm10` FLOAT(10,6) NULL,
              `nvpm10` FLOAT(10,6) NULL,
              `vpm10` FLOAT(10,6) NULL,
              `nvpm25` FLOAT(10,6) NULL,
              `vpm25` FLOAT(10,6) NULL,
              `co` FLOAT(10,6) NULL,
              `o3` FLOAT(10,6) NULL,
              `so2` FLOAT(10,6) NULL,
              `temperature` FLOAT(10,6) NULL,
              `rh` FLOAT(10,6) NULL,
              `airpressure` FLOAT(10,6) NULL,
              `datestart` VARCHAR(100) NULL,
              `date_end` VARCHAR(100) NULL,
              `current` TINYINT NULL,
              `instrument_type` VARCHAR(35) NULL,
              `pm25`  FLOAT(10,6) ,
              `no` FLOAT(10,6),
              PRIMARY KEY (`id`),
              CONSTRAINT `FK_location_reading`
                FOREIGN KEY (`locationid`)
                REFERENCES `pollution-db2`.`locationentity` (`idlocation_entity`)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION)
            ENGINE = InnoDB;

            """


    cur.execute(reading)
    conn.commit()


    schema = [
        {
            'measure': 'Date Time',
            'desc': 'Date and time of measurement',
            'unit': 'datetime'

    },
        {
            'measure': 'NOx',
            'desc': 'Concentration of oxides of nitrogen',
            'unit': '㎍/m3'

        },

        {   'measure': 'NO2',
            'desc': 'Concentration of nitrogen dioxide',
            'unit': '㎍/m3'

        },

        {   'measure': 'NO',
            'desc': 'Concentration of nitric oxide',
            'unit': '㎍/m3'

        },

        {   'measure': 'SiteID',
            'desc': 'Site ID for the station',
            'unit': 'integer'
        },

        {   'measure': 'PM10',
            'desc': 'Concentration of particulate matter <10 micron diameter',
            'unit': '㎍/m3'

        },

        {   'measure': 'NVPM10',
            'desc': 'Concentration of non - volatile particulate matter <10 micron diameter',
            'unit': '㎍/m3'

        },

        {   'measure': 'VPM10',
            'desc': 'Concentration of volatile particulate matter <10 micron diameter',
            'unit': '㎍/m3'

        },

        {   'measure': 'NVPM2.5',
            'desc': 'Concentration of non volatile particulate matter <2.5 micron diameter',
            'unit': '㎍/m3'

        },

        {   'measure': 'VPM10',
            'desc': 'Concentration of volatile particulate matter <10 micron diameter',
            'unit': '㎍/m3'
        },

        {   'measure': 'PM2.5',
            'desc': 'Concentration of particulate matter <2.5 micron diameter',
            'unit': '㎍/m3'

        },
        {   'measure': 'VPM2.5',
            'desc': 'Concentration of volatile particulate matter <2.5 micron diameter',
            'unit': '㎍/m3'

        },
        {   'measure': 'CO',
            'desc': 'Concentration of carbon monoxide',
            'unit': '㎎/m3'
        },

        {   'measure': 'O3',
            'desc': 'Concentration of ozone',
            'unit': '㎍/m3'
        },
        {   'measure': 'SO2',
            'desc': 'Concentration of sulphur dioxide',
            'unit': '㎍/m3'
        },
        {   'measure': 'Temperature',
            'desc': 'Air temperature',
            'unit': '°C'
        },

        {   'measure': 'RH',
            'desc': 'Relative Humidity',
            'unit': '%'
          },

        {   'measure': 'Air Pressure',
            'desc': 'Air Pressure',
            'unit': 'mbar'

          },

         {  'measure': 'Location',
            'desc': 'Text description of location',
            'unit': 'text'

            },


          { 'measure': 'geo_point_2d',
            'desc': 'Latitude and longitude',
            'unit': 'geo point'

            },

            {   'measure': 'DateStart',
                'desc': 'The date monitoring started',
            'unit': 'datetime'

            },

            {'measure': 'DateEnd',
            'desc': 'The date monitoring ended',
            'unit': 'datetime'

             },


             {'measure': 'Current',
            'desc': '	Is the monitor currently operating',
            'unit': 'text'

              },

            {'measure': 'Instrument Type',
            'desc': 'Classification of the instrument',
            'unit': 'text'

          }




    ]
    streets = [

        {'street_id': 188, 'street_name': 'AURN Bristol Centre', 'geo_point_2d': None},
        {'street_id': 203, 'street_name': 'Brislington Depot', 'geo_point_2d': '51.4417471802,-2.5599558322'},
        {'street_id': 206, 'street_name': 'Rupert Street', 'geo_point_2d': '51.4554331987,-2.59626237324'},
        {'street_id': 209, 'street_name': 'IKEA M32', 'geo_point_2d': None},
        {'street_id': 213, 'street_name': 'Old Market', 'geo_point_2d': '51.4560189999,-2.58348949026'},
        {'street_id': 215, 'street_name': 'Parson Street School', 'geo_point_2d': '51.432675707,-2.60495665673'},
        {'street_id': 228, 'street_name': 'Temple Meads Station', 'geo_point_2d': None},
        {'street_id': 270, 'street_name': 'Wells Road', 'geo_point_2d': '51.4278638883,-2.56374153315'},
        {'street_id': 271, 'street_name': 'Trailer Portway P&R', 'geo_point_2d': None},
        {'street_id': 375, 'street_name': 'Newfoundland Road Police Station',
         'geo_point_2d': '51.4606738207,-2.58225341824'},
        {'street_id': 395, 'street_name': "Shiner's Garage", 'geo_point_2d': '51.4577930324,-2.56271419977'},
        {'street_id': 452, 'street_name': 'AURN St Pauls', 'geo_point_2d': '51.4628294172,-2.58454081635'},
        {'street_id': 447, 'street_name': 'Bath Road', 'geo_point_2d': '51.4425372726,-2.57137536073'},
        {'street_id': 459, 'street_name': 'Cheltenham Road \ Station Road',
         'geo_point_2d': '51.4689385901,-2.5927241667'},
        {'street_id': 463, 'street_name': 'Fishponds Road', 'geo_point_2d': '51.4780449714,-2.53523027459'},
        {'street_id': 481, 'street_name': 'CREATE Centre Roof', 'geo_point_2d': '51.447213417,-2.62247405516'},
        {'street_id': 500, 'street_name': 'Temple Way', 'geo_point_2d': '51.4579497129,-2.58398909033'},
        {'street_id': 501, 'street_name': 'Colston Avenue', 'geo_point_2d': '51.4552693825,-2.59664882861'},
    ]

    for street in streets:
        sql_insert = "INSERT INTO `pollution-db2`.`locationentity` (idlocation_entity, name, geo_point_2d) VALUES (%s, '%s','%s')" % (
            street['street_id'], conn.escape_string(street['street_name']), str(street['geo_point_2d']))
        cur.execute(sql_insert)
        conn.commit()
    # Read CSV File
    csv_d = open('clean.csv', 'r')
    csv_data_file = csv.reader(csv_d, delimiter=";")
    next(csv_data_file)
    # insert CSV files into my SQL Database
    for air_reading in csv_data_file:
        sql_insert = "INSERT INTO `pollution-db2`.`reading` (nox, no2, locationid, datetime, pm10,nvpm10, vpm10, nvpm25, vpm25, co, o3,so2, temperature, rh, airpressure, datestart,date_end, current, instrument_type,pm25, no) VALUES({},{},{},'{}',{},{},{},{},{},{},{},{},{},{},{},'{}','{}',{},'{}',{}, {})".format(

            float(air_reading[2]) if air_reading[2] else 0,
            float(air_reading[3]) if air_reading[3] else 0,
            int(air_reading[5]),
            air_reading[1],
            float(air_reading[6]) if air_reading[6] else 0,
            float(air_reading[7]) if air_reading[7] else 0,
            float(air_reading[8]) if air_reading[8] else 0,
            float(air_reading[9]) if air_reading[9] else 0,
            float(air_reading[11]) if air_reading[11] else 0,
            float(air_reading[12]) if air_reading[12] else 0,
            float(air_reading[13]) if air_reading[13] else 0,
            float(air_reading[14]) if air_reading[14] else 0,
            float(air_reading[15]) if air_reading[15] else 0,
            float(air_reading[16]) if air_reading[16] else 0,
            float(air_reading[17]) if air_reading[17] else 0,
            air_reading[21],
            air_reading[20],
            bool(air_reading[22]),
            air_reading[23],
            float(air_reading[10]) if air_reading[10] else 0,
            float(air_reading[4]) if air_reading[4] else 0,
         )
        cur.execute(sql_insert)
        conn.commit()
    conn.close()
# catch and report any exception
except BaseException as err:

    print(f"An error occured: {err}")
    sys.exit(1)

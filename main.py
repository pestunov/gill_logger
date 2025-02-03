import serial
from datetime import datetime as dt
import pandas as pd
import numpy as np
import pymysql as db

import my_secure as ms

PERIOD_DATA_STORE = 10  # in minutes


def get_control_sum(line):
    csum = 0
    for b in line:
        csum ^= b
    return csum


def hex2int(byte):
    return byte - 48 if byte <= 59 else byte - 55


def handle_serial_line(line):
    """ check for ref bytes
        strip line
        check control sum """
    # check reference bytes
    if len(line) < 10:
        return None
    if (line[0] != 2) and (line[-5] != 3) and (line[-2] != 13) and (line[-1] != 10):
        return None
    body = line[1:-5]
    csum = hex2int(line[-4]) * 16 + hex2int(line[-3])
    # print(get_control_sum(body), csum)
    if get_control_sum(body) != csum:
        return None
    return body.decode()


def float_or_null(val):
    return float(val) if val == val else 'NULL'


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    columns = ['Q',
               'WindDir', 'WindSpeed',
               'WindDirCor', 'WindSpeedCor',
               'Latitude', 'Longitude', 'Altitude',
               'DateTime',
               'Vss', 'Status',
               ]

    df = pd.DataFrame(columns=columns)
    isData = False

    while 1:
        with serial.Serial('COM1', 19200, timeout=1) as ser:
            serialLine = ser.readline()
            serialLine = handle_serial_line(serialLine)
            if serialLine is not None:
                isData = True
                serialLine = serialLine.strip(',')
                data = serialLine.split(',')
                # data[5] is joined with ":" like here: "+56.478236:+085.054112:+0177.00"
                data.insert(6, '')
                data.insert(6, '')
                if data[5] != '':
                    subdata = data[5].split(':')
                    data[5] = subdata[0]
                    data[6] = subdata[1]
                    data[7] = subdata[2]
                df.loc[len(df), :] = data

        t = dt.now()
        # collect data for the period PERIOD_DATA_STORE and handle in with half shift
        if isData and t.second == 0 and t.minute in range(int(PERIOD_DATA_STORE/2), 61, PERIOD_DATA_STORE):
            isData = False

            # handle data array. write data to db
            df['DateTime'] = pd.to_datetime(df['DateTime'], yearfirst=True)
            # translate string to numeric
            cols = ['WindDir', 'WindSpeed', 'WindDirCor', 'WindSpeedCor', 'Longitude', 'Latitude', 'Altitude', 'Vss']
            for col in cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # calculate average wind speed and direction
            df['dx'] = np.sin(np.radians(df['WindDir'])) * df['WindSpeed']
            df['dy'] = np.cos(np.radians(df['WindDir'])) * df['WindSpeed']
            dx = df['dx'].mean()
            dy = df['dy'].mean()
            windSpeed = float_or_null(np.sqrt(dx * dx + dy * dy))
            windDir = float_or_null(np.rad2deg(np.arctan2(dx, dy)))

            # calculate average corrected wind direction
            df['dx'] = np.sin(np.radians(df['WindDirCor'])) * df['WindSpeedCor']
            df['dy'] = np.cos(np.radians(df['WindDirCor'])) * df['WindSpeedCor']
            dx = df['dx'].mean()
            dy = df['dy'].mean()
            windSpeedCor = float_or_null(np.sqrt(dx * dx + dy * dy))
            windDirCor = float_or_null(np.rad2deg(np.arctan2(dx, dy)))

            latitude = float_or_null(df['Latitude'].mean())
            longitude = float_or_null(df['Longitude'].mean())
            altitude = float_or_null(df['Altitude'].mean())
            vss = float_or_null(df['Vss'].mean())
            dateTimeGill = df['DateTime'].mean().strftime('"%Y-%m-%d %H-%M-%S"')
            dateTime = (t - pd.to_timedelta(f'{PERIOD_DATA_STORE / 2}m')).strftime('"%Y-%m-%d %H-%M-%S"')
            # clear df
            df = pd.DataFrame(columns=columns)

            # make result dictionary
            res = {'DateTime': dateTime,
                   'DateTimeGill': dateTimeGill,
                   'Longitude': longitude,
                   'Latitude': latitude,
                   'Altitude': altitude,
                   'WindSpeed': windSpeed,
                   'WindDir': windDir,
                   'WindSpeedCor': windSpeedCor,
                   'WindDirCor': windDirCor,
                   'Vss': vss,
                   }

            print(res)

            tableName = 'table'
            cols = f'{", ".join([str(k) for k in res.keys()])}'
            vals = f'{", ".join([str(v) for v in res.values()])}'

            print(cols, vals)
            with db.connect(host=ms.db_host,
                            user=ms.db_user,
                            password=ms.db_password,
                            port=ms.db_port,
                            db='baikal',) as con:
                cur = con.cursor()

                request = f'insert into `{tableName}` ({cols}) values ({vals});'
                print(request)
                cur.execute(request)

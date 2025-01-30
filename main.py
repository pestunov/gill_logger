import serial
from datetime import datetime
import pandas as pd
import numpy as np
# import sqlalchemy as db

PERIOD_DATA_STORE = 1  # in minutes

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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    columns = ['Q', 'WindDir', 'WindSpeed',
               'WindDirCor', #'WindSpeedCor',
               'Longitude', 'Latitude',
               'DateTime',
               'Vss', 'Status',
               ]
    df = pd.DataFrame(columns=columns)
    while 1:
        with serial.Serial('COM1', 19200, timeout=1) as ser:
            serialLine = ser.readline()
            # print(serialLine)
            strr = handle_serial_line(serialLine)
            if strr is not None:
                strr = strr.strip(',')
                data = strr.split(',')
                # print(data)
                df.loc[len(df), :] = data

        t = datetime.now()
        if t.second == 0 and t.minute % PERIOD_DATA_STORE == 0:
            # print(t.minute)
            df['DateTime'] = pd.to_datetime(df['DateTime'], yearfirst=True)

            # translate string to numeric
            cols = ['WindDir', 'WindSpeed', 'WindDirCor', 'Longitude', 'Latitude', 'Vss']
            for col in cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # calculate average wind speed and direction
            df['dx'] = np.sin(np.radians(df['WindDir'])) * df['WindSpeed']
            df['dy'] = np.cos(np.radians(df['WindDir'])) * df['WindSpeed']
            dx = np.mean(df['dx'])
            dy = np.mean(df['dy'])
            WindSpeed = np.sqrt(dx * dx + dy * dy)
            WindDir = np.rad2deg(np.arctan2(dx, dy))
            res = {'DateTime': pd.to_datetime(t - pd.to_timedelta(f'{PERIOD_DATA_STORE/2}m')),  #
                   'WinSpeed': WindSpeed,
                   'WindDir': WindDir,
                   }

            print(df)
            print(res)

            df = pd.DataFrame(columns=columns)


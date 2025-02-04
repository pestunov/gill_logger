import sqlalchemy as db
import my_secure as ms

engine = db.create_engine(f'mysql+pymysql://{ms.db_user}:{ms.db_password}@{ms.db_host}:{ms.db_port}/baikal')

metadata = db.MetaData()
table = db.Table('table2', metadata,
                 db.Column('i', db.Integer, primary_key=True),
                 db.Column('DateTime', db.DateTime),
                 db.Column('DateTimeGill', db.DateTime),
                 db.Column('WindSpeed', db.Double),
                 db.Column('WindDir', db.Double),
                 db.Column('WindSpeedMax', db.Double),
                 db.Column('WindDirMax', db.Double),
                 db.Column('WindSpeedCor', db.Double),
                 db.Column('WindDirCor', db.Double),
                 db.Column('WindSpeedCorMax', db.Double),
                 db.Column('WindDirCorMax', db.Double),
                 db.Column('Longitude', db.Double),
                 db.Column('Latitude', db.Double),
                 db.Column('Altitude', db.Double),
                 db.Column('Vss', db.Double),
                 )

metadata.create_all(engine)

import dbf
from pathlib import Path
import glob
import psycopg2
import sys


# user = sys.argv[1]
# database = sys.argv[2]
# host = sys.argv[3]

def dbf_to_pg(user, database, host):
    a=[]
    # open database connection
    try:
        conn = psycopg2.connect(user=user, host=host, database=database,
                                port="5432", client_encoding='utf8')
    except (Exception, psycopg2.Error) as err:
        print(err)
        # set connection to None if can`t connect
        conn = None

    if conn is not None:
        table_no = 0
        cursor = conn.cursor()

        for path in glob.iglob(f'C:/Users/USER IT-04/Desktop/21-02-2020_0_/*.dbf'):
        # for path in glob.iglob(f'{os.getcwd()}/*.dbf'):
            #get table name from dbf
            table_name = Path(path).stem
            table = dbf.Table(str(path), codepage='mac_roman')
            table.open()

            try:
                # create table in database
                cursor.execute(f"CREATE TABLE {table_name} ()")
                table_no +=1
            except (Exception, psycopg2.Error) as err:
                # print error
                print(err)

            for i in table.structure():
                # get column name
                column_name = i.split(' ')[0]

                # Change table name USER (Postgres name spaces conflict)
                if column_name == "USER" or column_name == "user":
                    column_name = "users"

                # get column data type
                column_type = i.split(' ')[1][0]
                a.append(column_type)

                # get column attribute
                column_attr = i.split(' ')[1][1::]

                try:
                    # convert tables columns data types from FoxPro to Postgres
                    if column_type == "N":
                        # format tuple from string(get column attr)
                        tp = tuple(int(num) for num in column_attr.replace('(', '').replace(')', '')
                                                                    .replace('...', '').split(','))
                        # data type length
                        length = tp[0]

                        # decimal length
                        decimal_length = tp[1]

                        # add table columns
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} NUMERIC({length},{decimal_length});")

                    elif column_type == "C":

                        # format tuple from string
                        tp = tuple(int(num) for num in column_attr.replace('(', '').replace(')', '')
                                                                    .replace('...', '').split(','))
                        # get data type length
                        length = tp[0]

                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} VARCHAR({length+300});")
                    elif column_type == 'D':
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} DATE;")
                    if column_type == 'T':
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} TIMESTAMP;")
                    elif column_type == 'L':
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} BOOLEAN;")
                    elif column_type == 'G':
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} VARCHAR(512);")
                    elif column_type == 'M':
                        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} VARCHAR(512);")

                except Exception as err:
                    # print error
                    print(err)


            # get table values from dbf
            values = [list(row) for row in table]

            # get columns number and add '%s' for every column(INSERT)
            cols = table.field_count * '%s, '

            # remove last ',' from cols
            columns = cols[0:-2]

            # insert data into table
            for data in values:
                for index,value in enumerate(data):
                    # remove before and after spaces from data
                    # replace null with ''
                    if type(value)== str:
                        data[index] = value.strip().replace('\x00', '')
                try:
                    cursor.execute(f"INSERT INTO {table_name} VALUES({columns})", data)
                except (Exception, psycopg2.Error) as err:
                    print(f'Atentie!!! A aparut o eroare ->> {type(err)} - {err}')
                    print(f'Tabel: {table_name}')
                    print(f'Camp: {data}')
                    return f'Exit'

                    # rollback the previous transaction before starting another
                    # conn.rollback()

            # close table
            table.close()
        # commit
        conn.commit()
        # close cursor
        cursor.close()
        # close database connection
        conn.close()
        print(set(a))
        print(f"Done! Added {table_no} table(s) into {database} database!")

dbf_to_pg('postgres', 'lazy', '192.168.88.134')

import psycopg
import os

if os.environ.get('ORDERBOOKSERVER') != None and os.environ.get('ORDERBOOKSERVER') != '':
        mysqlhost = os.environ.get('ORDERBOOKSERVER')
else:
        mysqlhost = 'localhost'

conn = psycopg.connect("dbname=orderbook user=vmware")

mycursor = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name<>'orderbookindex' AND table_name<>'server' AND table_name<>'client' AND table_name<>'v4orderbookindex' AND table_name<>'v4server' AND table_name<>'v4client';")
list = mycursor.fetchall()
conn.commit()
for member in list:
        print("DROP TABLE "+member[0]+";")
        mycursor.execute("DROP TABLE "+member[0]+";")
        conn.commit()
mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='orderbookindex');")
recordexists = mycursor.fetchone()[0]
conn.commit()
if recordexists == True:
        print("DELETE FROM orderbookindex;")
        mycursor.execute("DELETE FROM orderbookindex;")
        conn.commit()
        print("DELETE FROM server;")
        mycursor.execute("DELETE FROM server;")
        conn.commit()
        print("DELETE FROM client;")
        mycursor.execute("DELETE FROM client;")
        conn.commit()
mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='v4orderbookindex');")
recordexists = mycursor.fetchone()[0]
conn.commit()
if recordexists == True:
        print("DELETE FROM v4orderbookindex;")
        mycursor.execute("DELETE FROM v4orderbookindex;")
        conn.commit()
        print("DELETE FROM v4server;")
        mycursor.execute("DELETE FROM v4server;")
        conn.commit()
        print("DELETE FROM v4client;")
        mycursor.execute("DELETE FROM v4client;")
        conn.commit()

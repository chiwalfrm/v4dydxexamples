import psycopg
import os

if os.environ.get('ORDERBOOKSERVER') != None and os.environ.get('ORDERBOOKSERVER') != '':
        mysqlhost = os.environ.get('ORDERBOOKSERVER')
else:
        mysqlhost = 'localhost'

conn = psycopg.connect("dbname=orderbook user=vmware")

mycursor = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name<>'orderbookindex' AND table_name<>'v4orderbookindex';")
conn.commit()
list = mycursor.fetchall()
for member in list:
        print("DROP TABLE "+member[0]+";")
        mycursor.execute("DROP TABLE "+member[0]+";")
        conn.commit()
mycursor = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='orderbookindex';")
conn.commit()
count = mycursor.fetchone()[0]
if count == 1:
        print("DELETE FROM orderbookindex;")
        mycursor.execute("DELETE FROM orderbookindex;")
        conn.commit()
mycursor = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='v4orderbookindex';")
conn.commit()
count = mycursor.fetchone()[0]
if count == 1:
        print("DELETE FROM v4orderbookindex;")
        mycursor.execute("DELETE FROM v4orderbookindex;")
        conn.commit()

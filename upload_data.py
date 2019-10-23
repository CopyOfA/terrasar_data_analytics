#get libraries
import pandas as pd
from bs4 import BeautifulSoup
import shapely.geometry as shp
import json
import datetime as dt
import getpass
import psycopg2


#connect to db
hs_file = r'/dsa/home/jnj.0667/PSDS3100OP3-3_jnj.0667/Day5/doc_hs.kml'
st_file = r'/dsa/home/jnj.0667/PSDS3100OP3-3_jnj.0667/Day5/doc_st.kml'

mypasswd = getpass.getpass()
connection = psycopg2.connect(database = 'nce', 
                              user = 'nce14', 
                              host = 'dbase.sgn.missouri.edu',
                              password = mypasswd)
del(mypasswd)
cursor = connection.cursor()


#truncate tables if already existing
SQL = "TRUNCATE TABLE nce14.terrasar_st CASCADE"
cursor.execute(SQL)

SQL = "TRUNCATE TABLE nce14.terrasar_hs CASCADE"
cursor.execute(SQL)


#parse data into the "st" table
sql = "INSERT INTO nce14.terrasar_st "
sql += "(pid, name, dttm, description, position, coords) VALUES "
sql += "(%s,%s,%s,%s,ST_GeomFromText(%s, 4326),ST_GeomFromText(%s, 4326))"

print('parsing file ' + st_file)
t0 = dt.datetime.now()
with open(st_file, 'r') as f, connection, connection.cursor() as cursor:
    soup = BeautifulSoup(f, 'lxml')
    for node in soup.select('placemark'):
        im_id = node.attrs['id']
        name = node.find('name').string
        
        pt = node.find('point').coordinates.string #lat,long
        pt = shp.Point(float(pt.split(',')[0]), float(pt.split(',')[1])).wkt
        
        poly = node.find('polygon').coordinates.string #lat,long lat,long, etc.
        poly = poly.split(' ')[:-1]
        poly = shp.Polygon([(float(i.split(',')[0]), float(i.split(',')[1])) for i in poly]).wkt
        
        date = ' '.join(name.split('_',1)[1].replace('_',':').split('T'))
        date = date.split(':')[0] + ':' + date.split(':')[1]
        
        t = BeautifulSoup(node.find('description').string, 'html')
        a = {}
        for tr in t.find_all('tr'):
            b = [i.contents for i in tr.find_all('td')]
            try:
                a[b[0][0].split(':')[0]] = b[1][0]
            except:
                a[b[0][0].split(':')[0]] = 'NULL'
        a['Quicklook'] = str(a['Quicklook'])
        row = (im_id,name,date,json.dumps(a),pt,poly)
        cursor.execute(sql, row)

print("data loaded in " + str(dt.datetime.now() - t0))


#parse data into "hs" table
sql = "INSERT INTO nce14.terrasar_hs "
sql += "(pid, name, dttm, description, position, coords) VALUES "
sql += "(%s,%s,%s,%s,ST_GeomFromText(%s, 4326),ST_GeomFromText(%s, 4326))"

print('parsing file ' + hs_file)
t0 = dt.datetime.now()

with open(hs_file, 'r') as f, connection, connection.cursor() as cursor:
    soup = BeautifulSoup(f, 'lxml')
    for node in soup.select('placemark'):
        im_id = node.attrs['id']
        name = node.find('name').string
        
        pt = node.find('point').coordinates.string #lat,long
        pt = shp.Point(float(pt.split(',')[0]), float(pt.split(',')[1])).wkt
        
        poly = node.find('polygon').coordinates.string #lat,long lat,long, etc.
        poly = poly.split(' ')[:-1]
        poly = shp.Polygon([(float(i.split(',')[0]), float(i.split(',')[1])) for i in poly]).wkt
        
        date = ' '.join(name.split('_',1)[1].replace('_',':').split('T'))
        date = date.split(':')[0] + ':' + date.split(':')[1]
        
        t = BeautifulSoup(node.find('description').string, 'html')
        a = {}
        for tr in t.find_all('tr'):
            b = [i.contents for i in tr.find_all('td')]
            try:
                a[b[0][0].split(':')[0]] = b[1][0]
            except:
                a[b[0][0].split(':')[0]] = 'NULL'
        a['Quicklook'] = str(a['Quicklook'])
        row = (im_id,name,date,json.dumps(a),pt,poly)
        cursor.execute(sql, row)
                       
print("data loaded in " + str(dt.datetime.now() - t0))

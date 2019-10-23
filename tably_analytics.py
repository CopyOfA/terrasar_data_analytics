%matplotlib inline
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


## export data from the database into a GeoPandas that include all TerraSAR-X ST mode captures over Italy
#connect to geospatial countries database
geo = psycopg2.connect(database="dsa_ro", user="dsa_ro_user",password="readonly",host="dbase")
sql = "SELECT * FROM geospatial.country_borders WHERE name='Italy'"
italy = gpd.GeoDataFrame.from_postgis(sql, con=geo, geom_col='the_geom')

sql = "SELECT * FROM nce14.terrasar_st WHERE ST_WITHIN(position, ST_GeomFromText('"
sql += str(italy.the_geom[0]) + "', 4326))"
df = gpd.GeoDataFrame.from_postgis(sql, con=connection, geom_col='position')

ax = italy.plot(figsize=(18,18))
df.plot(ax=ax, color='red', marker='o', markersize=5, alpha=0.9)



##visualize the image capture foot prints over the outline of Italy
df = gpd.GeoDataFrame.from_postgis(sql, con=connection, geom_col='coords')
ax = italy.plot(figsize=(18,18))
df.plot(ax=ax, color='k')


##determine the counts per Country code, by ISO-3, of all the data in the TerraSAR-X ST and separately, HS
#First, collects for TerraSAR-X ST
t0 = dt.datetime.now()

sql = "SELECT cb.name, COUNT(*) FROM geospatial.country_borders cb "
sql += "JOIN nce14.terrasar_st st ON ST_WITHIN(st.position, cb.the_geom) "
sql += "GROUP BY cb.name ORDER BY COUNT(*) DESC LIMIT 20"
df = pd.read_sql(sql=sql, con=connection)

print("Total processing time: {}".format(dt.datetime.now()-t0))
print(df)

#Next, collects for TerraSAR-X HS
t0 = dt.datetime.now()

sql = "SELECT cb.name, COUNT(*) FROM geospatial.country_borders cb "
sql += "JOIN nce14.terrasar_hs hs ON ST_WITHIN(hs.position, cb.the_geom) "
sql += "GROUP BY cb.name ORDER BY COUNT(*) DESC LIMIT 20"
df = pd.read_sql(sql=sql, con=connection)

print("Total processing time: {}".format(dt.datetime.now()-t0))
print(df)



##total collects from TerraSAR-X ST organized by permutations of: Polarisation Channels, Mission, 
##Path direction, Looking Direction
sql = "SELECT description->'Polarisation Channels' as polarisation_channels, description->'Mission' as mission,"
sql += "description->'Path direction' as path_direction, description->'Looking Direction' as looking_direction, "
sql += "COUNT(*) FROM nce14.terrasar_st "
sql += "GROUP BY ROLLUP(polarisation_channels, mission, path_direction, looking_direction) ORDER BY COUNT(*) DESC"

df = pd.read_sql(sql=sql, con=connection)
print(df)


##show the total collects from TerraSAR-X ST and HS organized by permutations of: ST or HS, Polarisation Channels,
##Mission, Path direction, Looking Direction
sql = "SELECT description->'Polarisation Channels' as polarisation_channels, description->'Mission' as mission,"
sql += "description->'Path direction' as path_direction, description->'Looking Direction' as looking_direction, "
sql += "description->'Imaging Mode' as imaging_mode, COUNT(*) FROM "
sql += "(SELECT * FROM nce14.terrasar_st WHERE description->'Imaging Mode' IS NOT NULL "
sql += "UNION SELECT * FROM nce14.terrasar_hs WHERE description->'Imaging Mode' IS NOT NULL) tmp "
sql += "GROUP BY ROLLUP(polarisation_channels, mission, path_direction, looking_direction, imaging_mode) "
sql += "ORDER BY imaging_mode, COUNT(*) DESC"

df = pd.read_sql(sql=sql, con=connection)
print(df)


##Combine the data layers from the TerraSAR-X ST and the geospatial.geonames table to find the most imaged country
sql = "SELECT full_name, COUNT(*) FROM geospatial.geonames_feature gn JOIN nce14.terrasar_st st ON "
sql += "ST_WITHIN(gn.coords, st.coords) "
sql += "GROUP BY gn.full_name ORDER BY COUNT(*) DESC LIMIT 5"

df = pd.read_sql(sql=sql, con=connection)
print(df)


##Combine the data layers from the TerraSAR-X HS and the geospatial.gadm_admin_borders table to find the following items:
## 1. The most often imaged first order administrative division (name_1)
## 2.Include the number of captures from TerraSAR for HS over that first order administrative division
## 3. Include the total area of the captures over that first order administrative division, limited to just the area 
##    of the first order administrative division and not outside of it.
sql = "SELECT gab.iso, gab.name_1, COUNT(*) as total_captures, "
sql += "SUM(ST_AREA(ST_INTERSECTION(gab.the_geom,hs.coords))) as total_area "
sql += "FROM geospatial.gadm_admin_borders gab, nce14.terrasar_hs hs, "
sql += "(SELECT iso, name_1, COUNT(*) as count "
sql += "FROM geospatial.gadm_admin_borders gab JOIN nce14.terrasar_hs hs "
sql += "ON ST_WITHIN(hs.position, gab.the_geom) "
sql += "GROUP BY iso,gab.name_1 ORDER BY COUNT(*) DESC LIMIT 1) tmp "
sql += "WHERE gab.iso=tmp.iso AND gab.name_1=tmp.name_1 "
sql += "GROUP BY gab.iso, gab.name_1"

df = pd.read_sql(sql=sql, con=connection)
print(df)


##Visualize the yearly capture counts per country for the countries with the top 5 total captures
sql = "SELECT cb.name,CAST(EXTRACT(YEAR FROM t.dttm) AS INT) AS year,COUNT(*) "
sql += "FROM geospatial.country_borders cb "
sql += "JOIN (SELECT * FROM nce14.terrasar_st UNION SELECT * FROM nce14.terrasar_hs) t "
sql += "ON ST_WITHIN(t.position, cb.the_geom) "
sql += "WHERE cb.name IN "
sql += "(SELECT cb.name FROM geospatial.country_borders cb JOIN"
sql += "(SELECT * FROM nce14.terrasar_st UNION SELECT * FROM nce14.terrasar_hs) t "
sql += "ON ST_WITHIN(t.position, cb.the_geom) "
sql += "GROUP BY cb.name ORDER BY COUNT(*) DESC LIMIT 5) "
sql += "GROUP BY cb.name, year ORDER BY cb.name, year DESC"

df = pd.read_sql(sql=sql, con=connection)
df1 = df.pivot_table(columns='year', fill_value='count', index='name', aggfunc='sum')
df1.plot(kind='bar', figsize=(18,10))

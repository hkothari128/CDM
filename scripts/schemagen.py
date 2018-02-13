import psycopg2
import csv


conn_string = "host='localhost' dbname='MyCDM' user='developer' password='developer'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
tables=cursor.fetchall()
#print(tables)
tables=[str(i).split("'")[1] for i in tables]
#print(tables)
with open('my_cdm.csv', 'w') as csvfile:
	fieldnames = ['TABLE_NAME',	'COLUMN_NAME','IS_NULLABLE','DATA_TYPE','DESCRIPTION']
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()
	for table in tables:
		print(table)
		cursor.execute("select * FROM information_schema.columns where table_name='"+ table +"'")
		res=[row for row in cursor.fetchall()]
		for row in res:
			
			writer.writerow({fieldnames[0]:str(row[2]).upper(),fieldnames[1]:str(row[3]).upper(),
				fieldnames[2]:str(row[6]).upper(),fieldnames[3]:str(row[7]).upper(),fieldnames[4]:""})
			#print (str(row[2])+"  "+str(row[3])+"  "+str(row[6])+"  "+str(row[7]).upper())
		print("\n\n")
	
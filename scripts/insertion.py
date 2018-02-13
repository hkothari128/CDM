import csv
import psycopg2
conn_string = "host='localhost' dbname='MyCDM' user='developer' password='developer'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
source=""
mapping=list(tuple())
def read_map():
	with open("mapping.csv") as csv_file:
		reader=csv.reader(csv_file)
		for row in reader:
			mapping.append(row)
	mapping.remove(mapping[0])	
	#print(mapping)

def check(item,dest_table,dest_column):
	q="select count(*) from "+ dest_table+" where "+ dest_column+"='"+ item +"'"
	cursor.execute(q)
	result=cursor.fetchone()
	print(result[0])

def something():
	reader=csv.reader(source)
	headers=next(reader)
	
	for record in reader:

		for row in mapping:
			source_column=row[0]
			dest_column=row[2]
			dest_table=row[3]
			src_index=headers.index(source_column)
			item=record[src_index]
			check(item,dest_table,dest_column)
			#print(row)
			#print("item: ",record[column_number_in_src])
			
			#print(source_column,":",column_number_in_src)
		break
			
if __name__ == '__main__':
	path="ahs-mort-uttarakhand-nainital.csv"
	source=open(path)
	
	read_map()
	something()
	source.close()


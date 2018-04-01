import csv
import psycopg2
import os

conn_string = "host='localhost' dbname='MyCDM' user='developer' password='developer'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
source=""
mapping=list(tuple())
source_concept_id=None
def read_map():
	with open("mapping2.csv") as csv_file:
		reader=csv.reader(csv_file)
		for row in reader:
			mapping.append(row)
	mapping.remove(mapping[0])	
	#print(mapping)


def put_entity(item,source_column):
	if not check(item,"concept","concept_name")==0:
		print("ENTITY exists, but in another domain")
		q="select concept_id from concept where concept_name='"+ item +"'"
		cursor.execute(q)
		concept_id=cursor.fetchone()[0]
		print("SRC ",source_concept_id)
		q="insert into entity(entity_concept_id,identified_by,source_concept_id) values("+str(concept_id)+",'"+ item.lower() +"',"+ str(source_concept_id) +") returning id"
		cursor.execute(q)
		entity_id=cursor.fetchone()[0]

		add_measurement(item,item,source_column,"measurement_value_concept_id")
		
		conn.commit()

	elif check(item,"entity","identified_by")==0:
		print("adding new entity")
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(item,'Entity'))
		concept_id=cursor.fetchone()[0]
		print("SRC ",source_concept_id)

		q="insert into entity(entity_concept_id,identified_by,source_concept_id) values("+str(concept_id)+",'"+ item.lower() +"',"+ str(source_concept_id) +") returning id"
		cursor.execute(q)
		entity_id=cursor.fetchone()[0]

		add_measurement(item,item,source_column,"measurement_value_concept_id")
		
		conn.commit()

	else:
		print("ENTITY ALREADY EXISTS")

def add_concept(item,domain):
	if check_row([item,domain],"concept",["concept_name","domain_id"])==0:
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(item.lower(),domain.lower()))
		concept_id=cursor.fetchone()[0]
		print(concept_id)
		conn.commit()
		add_domain(domain)
	else:
		cursor.execute("select concept_id from concept where concept_name='"+ item.lower()+"' and domain_id='"+ domain.lower()+"'")
		concept_id=cursor.fetchone()[0]	
	return concept_id
		

def check(item,dest_table,dest_column):
	q="select count(*) from "+ dest_table.lower()+" where "+ dest_column.lower()+"='"+ item.lower() +"'"
	cursor.execute(q)
	result=cursor.fetchone()
	#print(result[0])
	return result[0]

def check_row(items,dest_table,dest_columns):
	q="select count(*) from "+ dest_table.lower()+" where "
	for i in range(len(items)-1):
		q+=dest_columns[i].lower()+"='"+ items[i].lower() +"' and "
	q+=dest_columns[-1].lower()+"='"+ items[-1].lower() +"'"		
	cursor.execute(q)
	result=cursor.fetchone()
	#print(result[0])
	return result[0]

def add_domain(domain):
	if check(domain,"domain","domain_id")==0:
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(domain,'Metadata'))
		concept_id=cursor.fetchone()[0]
		q="insert into domain values('"+ str(domain).lower() +"',"+ str(concept_id) +")"
		cursor.execute(q)
		conn.commit()

def get_domains():

	for row in mapping:
		domain=input("Enter domain for "+row[0]+" :")
		if not check(row[0],"concept","concept_name")==0 :
			q="select count(*) from  concept where concept_name= '"+ row[0] +"' and domain_id='"+ domain +"'"
			cursor.execute(q)
			result=cursor.fetchone()
			if(not result==0):
				print("this column is already a concept")
				continue
			else:
				add_domain(domain)

		else:
			add_domain(domain)	
			'''if check(domain,"domain","domain_id")==0:
				print("domain nai hai")
				q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
				cursor.execute(q,(domain,'Metadata'))
				concept_id=cursor.fetchone()[0]
				q="insert into domain values(%s,%s,"+ str(concept_id) +")"
				cursor.execute(q,(domain,domain))
				conn.commit()
				print(concept_id)
			else:
				print("domain hai")'''

		q="insert into concept(concept_name,domain_id)  values(%s,%s)"
		cursor.execute(q,(row[0].lower(),domain.lower()))
		conn.commit()
	
def temp_domains():
	from collections import OrderedDict
	domains=OrderedDict({'id':"identifier",
			'state':'location',
			'district':'location'})

	for i in domains:
		domain=domains[i].lower()
		
		if not check(i,"concept","concept_name")==0 :
			q="select count(*) from  concept where concept_name= '"+ i +"' and domain_id='"+ domain +"'"
			cursor.execute(q)
			result=cursor.fetchone()
			if(not result==0):
				print("this column(",i,") is already a concept")
				continue
			else:
				add_domain(domain)

		else:
			add_domain(domain)	
			

		q="insert into concept(concept_name,domain_id)  values(%s,%s)"
		cursor.execute(q,(i.lower(),domain.lower()))
		conn.commit()



def add_measurement(item,entity,source_column,dest_column):
	print("in measurements: ",entity)
	cursor.execute("select id from entity where identified_by = '"+ entity +"'")
	entity_id=cursor.fetchone()[0]
	print(source_column)
	cursor.execute("select concept_id from concept where concept_name='"+ source_column+"'")
	measurement_concept_id=cursor.fetchone()[0]

	if dest_column=="measurement_value_as_value":
		q="insert into measurements(entity_id,measurement_concept_id,measurement_value_as_value)\
		 values("+str(entity_id)+","+str(measurement_concept_id)+",'"+ item.lower() +"')"
		cursor.execute(q)
		conn.commit()
	elif dest_column=="measurement_value_concept_id":
		value=add_concept(item,"Measurement")
		#cursor.execute("select concept_id from concept where concept_name='"+ item+"' and domain_id='Measurement'")
		#value=cursor.fetchone()[0]
		print("MEASUREMENT_ID: ",measurement_concept_id)
		q="insert into measurements(entity_id,measurement_concept_id,measurement_value_concept_id)\
		 values("+str(entity_id)+","+str(measurement_concept_id)+","+ str(value).lower() +")"
		cursor.execute(q)
		conn.commit()



def insert():
	reader=csv.reader(source)
	
	headers=next(reader)
	headers=[header.lower().strip() for header in headers]
	print(headers[:5])
	count=1
	for record in reader:
		entity=""
		for row in mapping:
			source_column=row[0].lower()
			dest_column=row[2].lower()
			dest_table=row[3].lower()
			src_index=headers.index(source_column)
			item=record[src_index].lower()
			print(dest_table,dest_column,entity)
			if(dest_table=="entity" and dest_column=="identified_by"):
				entity=item
				put_entity(item,source_column)
				

			elif(dest_table=="measurements"):
				add_measurement(item,entity,source_column,dest_column)

			#print(row)
			#print("item: ",record[column_number_in_src])
			
			#print(source_column,":",column_number_in_src)
		
	
def initialize(file):
	source_table=file.lower()

	print ("\n\n\033[1;32;40m",source_table,"\033[0;37;40m")
	add_domain("Source")
	global source_concept_id
	source_concept_id = add_concept(source_table,"Source")
	cursor.execute("insert into source(source_name,source_concept_id,url) values('"+ source_table.lower() + "',"+ str(source_concept_id).lower() +",null)")
	conn.commit()	

'''if __name__ == '__main__':
	path="../data/mortality/"
	files=os.listdir(path)
	files=[f for f in files if ".py" not in f ]
	read_map()

	for file in files:
		if file.startswith("."):
			continue
		source=open(path+file)
		initialize(file)
		temp_domains()
		#get_domains()
		insert()
		
	source.close()	
'''
if __name__ == '__main__':
	path="../data/active/"
	file="No_Of_Still_Births_1.csv"
	read_map()
	source=open(path+file)
	initialize(file)
	#temp_domains()
	get_domains()
	insert()
		
	
		

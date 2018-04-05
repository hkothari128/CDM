import csv
import psycopg2
import os

conn_string = "host='localhost' dbname='myCDM' user='developer' password='developer'" # connection string
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()	
source=""				# file object for source file
mapping=list(tuple())	# stores mapping file
source_concept_id=None # stores concept_id of current file being inserted

def read_map():	# reads mapping file into a dictionary
	with open("mapping2.csv") as csv_file:
		reader=csv.reader(csv_file)
		for row in reader:
			mapping.append(row)
	mapping.remove(mapping[0])	
	


def put_entity(item,source_column):	# inserts an entity into the CDM
	if not check(item,"concept","concept_name")==0:	#if given item is not a concept
		#print("ENTITY exists, but in another domain")
		q="select concept_id from concept where concept_name='"+ item +"'"	#get concept_id for item
		cursor.execute(q)
		concept_id=cursor.fetchone()[0]
		#print("SRC ",source_concept_id)
		q="insert into entity(entity_concept_id,identified_by,source_concept_id) values("+str(concept_id)+",'"+ item.lower() +"',"+ str(source_concept_id) +") returning id"
		cursor.execute(q)
		entity_id=cursor.fetchone()[0]

		add_measurement(item,item,source_column,"measurement_value_concept_id")	# save entity as a measurement as well, since its also a mapped column
		
		conn.commit()

	elif check(item,"entity","identified_by")==0:	#  if item is a concept but not an entity 
		print("adding new entity")
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;" # add concept for that entity, domain 'ENTITY'
		cursor.execute(q,(item,'Entity'))
		concept_id=cursor.fetchone()[0]
		#print("SRC ",source_concept_id)

		q="insert into entity(entity_concept_id,identified_by,source_concept_id) values("+str(concept_id)+",'"+ item.lower() +"',"+ str(source_concept_id) +") returning id"
		cursor.execute(q)
		entity_id=cursor.fetchone()[0]

		add_measurement(item,item,source_column,"measurement_value_concept_id") # save entity as a measurement as well, since its also a mapped column
		
		conn.commit()

	else:
		print("ENTITY ALREADY EXISTS")

def add_concept(item,domain):	# adds a new concept to concept table
	if check_row([item,domain],"concept",["concept_name","domain_id"])==0:
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(item.lower(),domain.lower()))
		concept_id=cursor.fetchone()[0]
		conn.commit()
		add_domain(domain)	
	else:
		cursor.execute("select concept_id from concept where concept_name='"+ item.lower()+"' and domain_id='"+ domain.lower()+"'")
		concept_id=cursor.fetchone()[0]	
	return concept_id
		

def check(item,dest_table,dest_column):	# checks if a data item exists in a given column of given table, returns 0 if not exists
	q="select count(*) from "+ dest_table.lower()+" where "+ dest_column.lower()+"='"+ item.lower() +"'"
	cursor.execute(q)
	result=cursor.fetchone()
	return result[0]

def check_row(items,dest_table,dest_columns):	# check if a provided row exists in a provided table, returns 0 if not exists
	q="select count(*) from "+ dest_table.lower()+" where "
	for i in range(len(items)-1):
		q+=dest_columns[i].lower()+"='"+ items[i].lower() +"' and "
	q+=dest_columns[-1].lower()+"='"+ items[-1].lower() +"'"		
	cursor.execute(q)
	result=cursor.fetchone()
	return result[0]

def check_measurement(items,is_value=False):	# check if a provided row exists in a provided table, returns 0 if not exists
	q="select count(*) from measurements where entity_id= "+ str(items[0]) +" and measurement_concept_id= "+ str(items[1])
	if is_value:
		q+="and measurement_value_as_value='"+ str(items[2]) +"'"
	else:
		q+="and measurement_value_concept_id= "+ str(items[2])+""
	
	cursor.execute(q)
	result=cursor.fetchone()
	return result[0]

def add_domain(domain):		#adds a new domain
	if check(domain,"domain","domain_id")==0:	#checks if domain already exists
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"	#insert domain as a concept in concept table with domain 'METADATA'
		cursor.execute(q,(domain,'Metadata'))
		concept_id=cursor.fetchone()[0]
		q="insert into domain values('"+ str(domain).lower() +"',"+ str(concept_id) +")"		# insert domain into domain table
		cursor.execute(q)
		conn.commit()

def get_domains():		# obtains domains from user

	for row in mapping:
		domain=input("Enter domain for "+row[0]+" :")		#gets domain for source column item
		if not check(row[0],"concept","concept_name")==0 :  # checks if that item already exists
			q="select count(*) from  concept where concept_name= '"+ row[0] +"' and domain_id='"+ domain +"'"  #checks if the item has domain same as given domain
			cursor.execute(q)
			result=cursor.fetchone()
			if(not result==0):
				print("this column is already a concept")
				continue
			else:
				add_domain(domain)

		else:
			add_domain(domain)	
			

		q="insert into concept(concept_name,domain_id)  values(%s,%s)"
		cursor.execute(q,(row[0].lower(),domain.lower()))
		conn.commit()
	
def temp_domains():		# for batch insert
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



def add_measurement(item,entity,source_column,dest_column):			# adds measurement
	
	cursor.execute("select id from entity where identified_by = '"+ entity +"'")	# gets entity id
	entity_id=cursor.fetchone()[0]
	
	cursor.execute("select concept_id from concept where concept_name='"+ source_column+"'")	#gets concept id of what is to be measured
	measurement_concept_id=cursor.fetchone()[0]

	if dest_column=="measurement_value_as_value":	#if direct value is to be inserted into measurement
		if check_measurement([entity_id,measurement_concept_id,item.lower()],True)==0:
			q="insert into measurements(entity_id,measurement_concept_id,measurement_value_as_value)\
			 values("+str(entity_id)+","+str(measurement_concept_id)+",'"+ item.lower() +"')" 		
			cursor.execute(q)
			conn.commit()
	elif dest_column=="measurement_value_concept_id": #if a concept value is to be inserted into measurement
		value=add_concept(item,"Measurement")			#value to be inserted is made a concept and concept_id returned
		if check_measurement([entity_id,measurement_concept_id,value])==0:
			#cursor.execute("select concept_id from concept where concept_name='"+ item+"' and domain_id='Measurement'")
			#value=cursor.fetchone()[0]
			
			q="insert into measurements(entity_id,measurement_concept_id,measurement_value_concept_id)\
			 values("+str(entity_id)+","+str(measurement_concept_id)+","+ str(value).lower() +")"
			cursor.execute(q)
			conn.commit()



def insert():				#insertion script
	reader=csv.reader(source)		# read source file
	
	headers=next(reader)
	headers=[header.lower().strip() for header in headers]
	
	for record in reader:		# loop through every record in source file
		entity=""
		for row in mapping:		# check for mappings
			source_column=row[0].lower()
			dest_column=row[2].lower()
			dest_table=row[3].lower()
			src_index=headers.index(source_column)
			item=record[src_index].lower()	#find item in record for mapped column
			
			if(dest_table=="entity" and dest_column=="identified_by"):	# if entity is to be inserted
				entity=item
				put_entity(item,source_column)
				

			elif(dest_table=="measurements"):	# if measurement is to be inserted
				#if "state" in source_column:
					#print(item)
				add_measurement(item,entity,source_column,dest_column)

			
	
def initialize(file):					# add source file to source table
	source_table=file.lower()

	#print ("\n\n\033[1;32;40m",source_table,"\033[0;37;40m")
	add_domain("Source")				
	global source_concept_id
	source_concept_id = add_concept(source_table,"Source")	# get concept_id of added source_file
	if check(source_table,"source","source_name",)==0:
		cursor.execute("insert into source(source_name,source_concept_id,url) values('"+ source_table.lower() + "',"+ str(source_concept_id).lower() +",null)")
	conn.commit()	

'''if __name__ == '__main__':					# for batch insert
	path="../data/active/mortality/mort_new/"
	files=os.listdir(path)
	files=[f for f in files if ".py" not in f ]
	read_map()
	counter =0 
	for file in files:			
		if file.startswith("."):
			continue
		source=open(path+file)
		initialize(file)
		temp_domains()
		#get_domains()
		insert()
		counter+=1
		print("********************************* ",counter,"***********************************88")
	source.close()	'''

if __name__ == '__main__':
	path="../data/active/"
	file="No_Of_Still_Births_1.csv"
	read_map()
	source=open(path+file)
	initialize(file)
	#temp_domains()
	get_domains()
	insert()
		
	
		

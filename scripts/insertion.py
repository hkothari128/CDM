import csv
import psycopg2
conn_string = "host='localhost' dbname='MyCDM' user='developer' password='developer'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
source=""
mapping=list(tuple())
source_concept_id=None
def read_map():
	with open("mapping.csv") as csv_file:
		reader=csv.reader(csv_file)
		for row in reader:
			mapping.append(row)
	mapping.remove(mapping[0])	
	#print(mapping)


def put_entity(item):
	if check(item,"entity","identified_by")==0:
		print("adding new entity")
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(item,'Entity'))
		concept_id=cursor.fetchone()[0]
		print("SRC ",source_concept_id)
		q="insert into entity(entity_concept_id,identified_by,source_concept_id) values("+str(concept_id)+",'"+ item +"',"+ str(source_concept_id) +")"
		cursor.execute(q)
		conn.commit()

def add_concept(item,domain):
	if check(item,"concept","concept_name")==0:
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(item,domain))
		concept_id=cursor.fetchone()[0]
		print(concept_id)
		conn.commit()
		add_domain(domain)
	else:
		cursor.execute("select concept_id from concept where concept_name='"+ item+"' and domain_id='"+ domain+"'")
		concept_id=cursor.fetchone()[0]	
	return concept_id
		

def check(item,dest_table,dest_column):
	q="select count(*) from "+ dest_table+" where "+ dest_column+"='"+ item +"'"
	cursor.execute(q)
	result=cursor.fetchone()
	#print(result[0])
	return result[0]

def add_domain(domain):
	if check(domain,"domain","domain_id")==0:
		q="insert into concept(concept_name,domain_id)  values(%s,%s) returning concept_id;"
		cursor.execute(q,(domain,'Metadata'))
		concept_id=cursor.fetchone()[0]
		q="insert into domain values('"+ str(domain) +"',"+ str(concept_id) +")"
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
		cursor.execute(q,(row[0],domain))
		conn.commit()
	
		

def insert():
	reader=csv.reader(source)
	headers=next(reader)
	count=1
	for record in reader:
		entity=""
		for row in mapping:
			source_column=row[0]
			dest_column=row[2]
			dest_table=row[3]
			src_index=headers.index(source_column)
			item=record[src_index]
			print(dest_table,dest_column,entity)
			if(dest_table=="entity" and dest_column=="identified_by"):
				entity=item
				put_entity(item)
			elif(dest_table=="measurements"):
				print("in measurements: ",entity)
				cursor.execute("select id from entity where identified_by = '"+ entity +"'")
				entity_id=cursor.fetchone()[0]
				cursor.execute("select concept_id from concept where concept_name='"+ source_column+"'")
				measurement_concept_id=cursor.fetchone()[0]
				if dest_column=="measurement_value_as_value":
					q="insert into measurements(entity_id,measurement_concept_id,measurement_value_as_value)\
					 values("+str(entity_id)+","+str(measurement_concept_id)+",'"+ item +"')"
					cursor.execute(q)
					conn.commit()
				elif dest_column=="measurement_value_concept_id":
					value=add_concept(item,"Measurement")
					#cursor.execute("select concept_id from concept where concept_name='"+ item+"' and domain_id='Measurement'")
					#value=cursor.fetchone()[0]
					print("MEASUREMENT_ID: ",measurement_concept_id)
					q="insert into measurements(entity_id,measurement_concept_id,measurement_value_concept_id)\
					 values("+str(entity_id)+","+str(measurement_concept_id)+","+ str(value) +")"
					cursor.execute(q)
					conn.commit()

			#print(row)
			#print("item: ",record[column_number_in_src])
			
			#print(source_column,":",column_number_in_src)
		if(count==1):
			break
		count=count+1
	
def initialize():
	source_table=mapping[1][1]
	#print (source_table)
	add_domain("Source")
	global source_concept_id
	source_concept_id = add_concept(source_table,"Source")
	cursor.execute("insert into source(source_name,source_concept_id,url) values('"+ source_table + "',"+ str(source_concept_id) +",null)")
	conn.commit()		
if __name__ == '__main__':
	path="ahs-mort-uttarakhand-nainital.csv"
	source=open(path)
	
	read_map()
	initialize()
	get_domains()
	insert()
	#something()
	source.close()


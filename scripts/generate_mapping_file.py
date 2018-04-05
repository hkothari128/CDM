import json
import sys

from pprint import pprint
from pprint import pformat
import csv
from collections import OrderedDict
id_dict={}
map_table=[]


class mapper():	# class to model on record in mapping file
	src_column=""
	src_table=""
	dest_column=""
	dest_table=""

	def __init__(self, src,dst,src_table,dst_table):
		self.src_column=src
		self.dest_column=dst
		self.src_table=src_table
		self.dest_table=dst_table

	def get_src_col(self):
		return self.src_column	
			
	def get_dest_col(self):
		return self.dest_column	

	def display(self):
		print(self.src_column,"\t\t",self.src_table,"\t\t",self.dest_table,"\t\t",self.dest_column)

	def write_to_csv(self,writer):
		writer.writerow({'src_column':self.src_column, 'src_table':self.src_table,'dest_column':self.dest_column,'dest_table':self.dest_table})

def save_csv():
	with open('mapping.csv', 'w') as csvfile:
	    fieldnames = ['src_column', 'src_table','dest_column','dest_table']
	    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

	    writer.writeheader()
	    for mapping in map_table:
	    	#if(mapping.get_dest_col()=="identified_by"):
	    	#	csvfile.seek(1)
	    	

	    	#else:
	    	#	csvfile.seek(0,2)
	    	mapping.write_to_csv(writer)

	print("DONE")

def create_map_table(records):
	keys=records["tableMapToFieldToFieldMaps"]["@keys"]
	table_mappings=records["tableMapToFieldToFieldMaps"]["@items"]
	for i in range(len(keys)):
		src_table=id_dict[keys[i]["sourceItem"]["@ref"]]
		dst_table=id_dict[keys[i]["cdmItem"]["@ref"]]
		column_mappings=table_mappings[i]["@items"]
		for column_mapping in column_mappings:

			src=id_dict[column_mapping["sourceItem"]["@ref"]]
			dst=id_dict[column_mapping["cdmItem"]["@ref"]]
			
			obj=mapper(src,dst,src_table,dst_table)
			global map_table
			if(dst=="identified_by"):
				map_table = [obj] + map_table
			else:
				map_table.append(obj)
	
	print("src_column\t\tsrc_table\t\t\t\tdst_table\t\tdst_column")		
	for mapping in map_table:
		mapping.display()		


def get_ids(records):

	for field in records["sourceDb"]["tables"]["@items"]:
		item=field
		if("@id" in item):
			id_dict[item["@id"]]=item["name"]
		if "@items" in item["fields"]:
			items=item["fields"]["@items"]
			for item in items:
				if("@id" in item):
					id_dict[item["@id"]]=item["name"]

	for field in records["cdmDb"]:

		item=field
		
		if( "@id" in item):
			id_dict[records["cdmDb"]["@id"]]=records["cdmDb"]["dbName"]
		elif "tables" in item :
			tables=records["cdmDb"]["tables"]["@items"]
			for table in tables:
				item=table
				if("@id" in item):
					id_dict[item["@id"]]=item["name"]

				if "@items" in item["fields"]:
					items=item["fields"]["@items"]
					for item in items:
						if("@id" in item):
							id_dict[item["@id"]]=item["name"]
	pprint(id_dict)

def main():
	path="./test.json"
	with open(path,"r") as file:
		json_str=file.read()
		records=json.loads(json_str)
		
		get_ids(records)
		create_map_table(records)
		save_csv()
	
	

if __name__ == '__main__':
	main()	
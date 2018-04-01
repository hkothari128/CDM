import csv

write_file=open("geocode_refined.csv","w")
csv_writer=csv.writer(write_file)
with open("geocode_health_centre.csv") as csv_file:
	csvReader = csv.reader(csv_file)
	file=[]
	row=next(csvReader)
	name=row[0]
	counter=0
	for row in csvReader:

		while(row[0]==name):
			file.append(row)
			
		for r in file[:len(file)/10]:
			csv_writer.writerow(r)
		file=[]
		name=row[0]

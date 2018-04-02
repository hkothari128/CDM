import csv

write_file=open("geocode_refined.csv","w")
csv_writer=csv.writer(write_file)
with open("geocode_health_centre.csv") as csv_file:
	csvReader = csv.reader(csv_file)
	file=[]
	row=next(csvReader)
	row=next(csvReader)
	name=row[0]
	counter=0
	for row in csvReader:
		print(name)
		
		if(row[0]==name):
			file.append(row)
			counter+=1
			print(counter)
			continue
		
		counter=0
		l=len(file)/10
		for r in file[:int(l)]:
			csv_writer.writerow(r)
		file=[]
		name=row[0]
write_file.close()
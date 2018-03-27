import os
import csv
files=os.listdir()
files=[f for f in files if ".py" not in f ]
print(files,"\n\n\n\n")

for file in files:
	csv_file=[]
	with open(file) as csvfile:

		csvReader = csv.reader(csvfile)
		headers=next(csvReader)
		csv_file.append(headers)
		index=headers.index("is_death_associated_with_preg")
		for row in csvReader:
			if ("yes" in str(row[index]).lower()):
				csv_file.append(row)
		with open(file,'w') as wfile:
			csv_writer = csv.writer(wfile)
			csv_writer.writerows(csv_file)
			print(file)


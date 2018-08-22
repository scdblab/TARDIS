import sys

file = sys.argv[1]
outputfile = sys.argv[2]
row="dirty_docs\n"
recover=False
with open(file) as f:
	for line in f:
		if 'Back at' in line:
			recover=True
		if recover == True:
			if "Remaining dirty docs:" in line:
				thp = line.split(" ")[3]
				row += str(int(float(thp)))
				row += "\n"

text_file = open(outputfile, "w")
text_file.write(row)
text_file.close()

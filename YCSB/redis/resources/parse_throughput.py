import sys

file = sys.argv[1]
outputfile = sys.argv[2]
row="throughput\n"
with open(file) as f:
	for line in f:
		if "current ops/sec" in line:
			thp = line.split(";")[1]
			thp = thp.replace("current ops/sec", "")
			thp = thp.replace(" ", "")
			row += str(int(float(thp)))
			row += "\n"

text_file = open(outputfile, "w")
text_file.write(row)
text_file.close()
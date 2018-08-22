import sys

file = sys.argv[1]
action = sys.argv[2]
outputfile = sys.argv[3]
# Max=443135, Min=175, Avg=149821.26, 90=367359, 99=421119, 99.9=443135, 99.99
row="Avg,p90,p99\n"

def safe_float(n):
	try:
		return int(float(n))
	except:
		return 0

with open(file) as f:
	for line in f:
		if action in line:
			ems = line.split(";")[2].split(",")
			stats={}
			for i in range(0, len(ems)):
				if action in ems[i]:
					stats["Max"]=safe_float(ems[i+1].split("=")[1])
					stats["Min"]=safe_float(ems[i+2].split("=")[1])
					stats["Avg"]=safe_float(ems[i+3].split("=")[1])
					stats["p90"]=safe_float(ems[i+4].split("=")[1])
					stats["p99"]=safe_float(ems[i+5].split("=")[1])
					stats["p999"]=safe_float(ems[i+6].split("=")[1])
					stats["p9999"]=safe_float(ems[i+7].split("]")[0].split("=")[1])
					# row= row + str(stats["Max"]) + ","
					# row= row + str(stats["Min"]) + ","
					row= row + str(stats["Avg"]) + ","
					row= row + str(stats["p90"]) + ","
					row= row + str(stats["p99"]) + ","
					# row= row + str(stats["p999"]) + ","
					# row= row + str(stats["p9999"]) + ","
					row = row + "\n"

text_file = open(outputfile, "w")
text_file.write(row)
text_file.close()
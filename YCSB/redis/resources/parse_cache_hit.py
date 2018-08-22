import json
from pprint import pprint
import sys

file = sys.argv[1]
outputfile = sys.argv[2]
data = json.load(open(file))
cache_stats = data["cacheStats"]
timeline=[]
row=""
for cache_server in sorted(cache_stats):
	stats = cache_stats[cache_server]
	row+=cache_server
	row+=","

for time in cache_stats[cache_server]:
	timeline.append(int(time))
timeline.sort()
row+="\n"

for time in timeline:
	for cache_server in sorted(cache_stats):
		prev_get=0
		prev_hit=0
		if time > 0:
			prev_hit=int(cache_stats[cache_server][str(time-1)]["hit"])
			prev_get=int(cache_stats[cache_server][str(time-1)]["get"])
		hit=int(cache_stats[cache_server][str(time)]["hit"]) - prev_hit
		get=int(cache_stats[cache_server][str(time)]["get"]) - prev_get
		if get == 0:
			row+="-1"
		else:
			row+=str(float(hit*100)/float(get))
		row+=","
	row+="\n"

text_file = open(outputfile, "w")
text_file.write(row)
text_file.close()
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
	row+="-item-evict"
	row+=","
	row+=cache_server
	row+="-slab-evict"
	row+=","

for time in cache_stats[cache_server]:
	timeline.append(int(time))
timeline.sort()
row+="\n"

for time in timeline:
	for cache_server in sorted(cache_stats):
		prev_item_evict=0
		prev_slab_evict=0
		if time > 0:
			prev_item_evict=int(cache_stats[cache_server][str(time-1)]["item_evict"])
			prev_slab_evict=int(cache_stats[cache_server][str(time-1)]["slab_evict"])
		item_evict=int(cache_stats[cache_server][str(time)]["item_evict"]) - prev_item_evict
		slab_evict=int(cache_stats[cache_server][str(time)]["slab_evict"]) - prev_slab_evict
		row+=str(item_evict)
		row+=","
		row+=str(slab_evict)
		row+=","
	row+="\n"

text_file = open(outputfile, "w")
text_file.write(row)
text_file.close()
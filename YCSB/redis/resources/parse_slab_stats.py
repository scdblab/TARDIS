import json
from pprint import pprint
import sys

file = "/Users/haoyuh/Documents/PhdUSC/NVM-Recovery/results/new-paper-exp3b-nvcache-workloadd-coord-NORMAL-clientconfig-5_cache_no_migration-coorconfig-5_cache_recover_5min-1-clients-20-threads-1000000-warm-tr-1000000-600-120-cachesize-immediate-true-rd--1-migra--1-z/client-h1-metrics.json"
# outputfile = sys.argv[2]
data = json.load(open(file))
cache_stats = data["slabStats"]["h3:11211"]
timeline=[]
curr_row=""
evict_row=""
for i in range(len(cache_stats["0"])):
	curr_row+=str(i)
	curr_row+="-item-curr"
	curr_row+=","
	evict_row+=str(i)
	evict_row+="-item-evict"
	evict_row+=","

curr_row+="\n"
evict_row+="\n"

for time in cache_stats:
	timeline.append(int(time))
timeline.sort()

for time in timeline:
	for slabclass in range(len(cache_stats[str(time)])):
		prev_item_curr=0
		prev_item_evict=0
		if time > 0:
			prev_item_evict=int(cache_stats[str(time-1)][slabclass]["item_evict"])
			prev_item_curr=int(cache_stats[str(time-1)][slabclass]["item_curr"])
		item_evict=int(cache_stats[str(time)][slabclass]["item_evict"]) - prev_item_evict
		item_curr=int(cache_stats[str(time)][slabclass]["item_curr"]) - prev_item_curr
		evict_row+=str(item_evict)
		evict_row+=","
		curr_row+=str(item_curr)
		curr_row+=","
	evict_row+="\n"
	curr_row+="\n"

print evict_row
print curr_row
import json
from pprint import pprint
import sys

file = sys.argv[1]
outputfile = sys.argv[2]
data = json.load(open(file))

del data["cacheStats"]
del data["slabStats"]

with open(outputfile, "w") as outfile:
	json.dump(data, outfile)
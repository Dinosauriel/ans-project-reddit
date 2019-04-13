
import json

relevant_fields = [
	"body",
	"author_fullname",
	"created_utc",
	"link_id",
	"parent_id",
	"id",
	"subreddit_id",
]

source_file = open("/Volumes/Aurel 4TB/ANS/raw_data/RCOMMENTS_2019-01.json")
out_file = open("/Volumes/Aurel 4TB/ANS/raw_data/prep_RCOMMENTS_2019-01.json", "w")

for line in source_file:
	comment = json.loads(line)
	filtered_dict = {k:v for (k,v) in comment.items() if k in relevant_fields}
	out_file.write(json.dumps(filtered_dict) + "\n")

source_file.close()
out_file.close()


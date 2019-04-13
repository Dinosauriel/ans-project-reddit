
import env_file
import json

relevant_fields = [
	"author",
	"author_fullname",
	"author_created_utc",
	"created_utc",
	"subreddit",
	"subreddit_id",
	"title",
	"num_crossposts",
	"num_comments",
	"selftext"
]

source_file = open("/Users/aurelfeer/Downloads/RS_2018-11.json")
out_file = open("/Users/aurelfeer/Downloads/s_prep_RS_2018-11.json", "w")
#source_file = open("/Volumes/Aurel 4TB/ANS/raw_data/RSUBMISSIONS_2019-01.json")
#out_file = open("/Volumes/Aurel 4TB/ANS/raw_data/prep_RSUBMISSIONS_2019-01.json", "w")

n = 0
for line in source_file:
	comment = json.loads(line)
	filtered_dict = {
		"a": comment["author"],
		"af": comment["author_fullname"],
		"acu": comment["author_created_utc"],
		"cu": comment["created_utc"],
		"s": comment["subreddit"],
		"si": comment["subreddit_id"],
		"t": comment["title"],
		"ncr": comment["num_crossposts"],
		"nco": comment["num_comments"],
		"st": comment["selftext"]
	}
	out_file.write(json.dumps(filtered_dict) + "\n")
	n += 1
	if (n % 100_000 == 0):
		print(n)

source_file.close()
out_file.close()


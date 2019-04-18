
import mysql.connector
import env_file
import json
import os

relevant_fields = [
	"id",
	"author",
	"author_fullname",
	"author_created_utc",
	"created_utc",
	"subreddit",
	"subreddit_id",
	"title",
	"num_crossposts",
	"num_comments",
	"selftext",
	"crosspost_parent"
]

env = env_file.get()

db = mysql.connector.connect(
	host=env["MYSQL_HOST"],
	user=env["MYSQL_USER"],
	password=env["MYSQL_PW"],
	database=env["MYSQL_DB"],
	
	#raise_on_warnings=True
)
db.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')


cursor = db.cursor()
cursor.execute("DROP TABLE IF EXISTS submissions")
cursor.execute("CREATE TABLE submissions ( \
	id VARCHAR(15) PRIMARY KEY, \
	author VARCHAR(255), \
	author_fullname VARCHAR(15), \
	author_created_utc INT, \
	created_utc INT, \
	subreddit VARCHAR(63), \
	subreddit_id VARCHAR(15), \
	title TEXT, \
	num_crossposts INT, \
	num_comments INT, \
	selftext MEDIUMTEXT, \
	crosspost_parent VARCHAR(15), \
	INDEX (author_fullname), \
	INDEX (subreddit_id), \
	INDEX (crosspost_parent) \
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

insertion_sql = "INSERT INTO submissions (" + ", ".join(relevant_fields) + ") VALUES \
	(%(id)s, %(author)s, %(author_fullname)s, %(author_created_utc)s, \
	%(created_utc)s, %(subreddit)s, %(subreddit_id)s, %(title)s, \
	%(num_crossposts)s, %(num_comments)s, %(selftext)s, %(crosspost_parent)s)"

source_files = os.scandir(env["SUBM_JSON_DIR"])

n = 0
for file in source_files:
	source_file = open(file.path)
	for line in source_file:
		comment = json.loads(line)
		comment = {k:v for (k,v) in comment.items() if k in relevant_fields}
		if "author_fullname" not in comment:
			comment["author_fullname"] = None
		if "author_created_utc" not in comment:
			comment["author_created_utc"] = None
		if "crosspost_parent" not in comment:
			comment["crosspost_parent"] = None

		cursor.execute(insertion_sql, comment)
		n += 1
		if (n % 100000 == 0):
			db.commit()
			print(n)
	source_file.close()

db.commit()
db.close()

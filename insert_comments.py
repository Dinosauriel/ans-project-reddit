
import mysql.connector
import env_file
import json

relevant_fields = [
	"id",
	"author",
	"author_fullname",
	"author_created_utc",
	"created_utc",
	"subreddit_id",
	"body",
	"link_id",
	"parent_id"
]

source_file = open("/mnt/6TB-RED/ansproj/raw_data/RCOMMENTS_2019-01.json")

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
cursor.execute("DROP TABLE IF EXISTS comments")
cursor.execute("CREATE TABLE comments ( \
	id VARCHAR(15) PRIMARY KEY, \
	author VARCHAR(255), \
	author_fullname VARCHAR(15), \
	author_created_utc INT, \
	created_utc INT, \
	subreddit_id VARCHAR(15), \
	body TEXT, \
	link_id VARCHAR(15), \
	parent_id VARCHAR(15), \
	INDEX(author_fullname), \
	INDEX(created_utc), \
	INDEX(link_id), \
	INDEX(parent_id) \
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

insertion_sql = "INSERT INTO comments (" + ", ".join(relevant_fields) + ") VALUES \
	(%(id)s, %(author)s, %(author_fullname)s, %(author_created_utc)s, \
	%(created_utc)s, %(subreddit_id)s, %(body)s, %(link_id)s, \
	%(parent_id)s)"
#c = []
#i = 0
n = 0
for line in source_file:
	#if n < 4800000:
	#	n += 1
	#	continue
	comment = json.loads(line)
	comment = {k:v for (k,v) in comment.items() if k in relevant_fields}
	if "author_fullname" not in comment:
		comment["author_fullname"] = "NULL"
	if "author_created_utc" not in comment:
		comment["author_created_utc"] = 0
	
	cursor.execute(insertion_sql, comment)
	#insert batches of 1000
	#i += 1
	#if i == 1000:
	#	cursor.executemany(insertion_sql, c)
	#	i = 0
	#	c = []
	n += 1
	if (n % 100000 == 0):
		db.commit()
		print(n)

#insert remaining
#cursor.executemany(insertion_sql, c)

db.commit()

db.close()
source_file.close()

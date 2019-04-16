
import mysql.connector
import env_file
import json

relevant_fields = [
	"id",
	"name",
	"created_utc",
	"display_name",
	"subscribers"
]

source_file = open("/mnt/6TB-RED/ansproj/raw_data/SUBREDDITS.json")

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
cursor.execute("DROP TABLE IF EXISTS subreddits")
cursor.execute("CREATE TABLE subreddits ( \
	id VARCHAR(15) PRIMARY KEY, \
	name VARCHAR(15), \
	created_utc INT, \
	display_name VARCHAR(63), \
	subscribers INT \
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

insertion_sql = "INSERT INTO subreddits (" + ", ".join(relevant_fields) + ") VALUES \
	(%(id)s, %(name)s, %(created_utc)s, %(display_name)s, \
	%(subscribers)s)"

n = 0
for line in source_file:
	#if n < 4800000:
	#	n += 1
	#	continue
	comment = json.loads(line)
	comment = {k:v for (k,v) in comment.items() if k in relevant_fields}
	
	cursor.execute(insertion_sql, comment)
	n += 1
	if (n % 100000 == 0):
		db.commit()
		print(n)

db.commit()

db.close()
source_file.close()

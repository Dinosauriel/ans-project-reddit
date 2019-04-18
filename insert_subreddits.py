
import mysql.connector
import env_file
import json
import os

relevant_fields = [
	"id",
	"name",
	"created_utc",
	"display_name",
	"subscribers"
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


source_files = os.scandir(env["SUBR_JSON_DIR"])

n = 0
for file in source_files:
	source_file = open(file.path)
	for line in source_file:
		comment = json.loads(line)
		comment = {k:v for (k,v) in comment.items() if k in relevant_fields}
		
		cursor.execute(insertion_sql, comment)
		n += 1
		if (n % 100000 == 0):
			db.commit()
			print(n)
	source_file.close()

db.commit()
db.close()
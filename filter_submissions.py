import mysql.connector
import env_file

env = env_file.get()

db = mysql.connector.connect(
	host=env["MYSQL_HOST"],
	user=env["MYSQL_USER"],
	password=env["MYSQL_PW"],
	database=env["MYSQL_DB"],
)
db.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')

cursor = db.cursor()
cursor.execute("DROP TABLE IF EXISTS reduced_submissions")
cursor.execute("CREATE TABLE reduced_submissions LIKE submissions")

db.commit()

cursor.execute("SELECT * FROM reduced_subreddits")
reduced_subreddits = cursor.fetchall()

n = 0
for subreddit in reduced_subreddits:
	(id, name, created_utc, display_name, subscribers) = subreddit
	print(subreddit)
	n += 1
	print("fetching...")
	cursor.execute("SELECT * FROM submissions WHERE subreddit_id = \"" + name + "\"")
	subms = cursor.fetchall()
	print("inserting...")
	for s in subms:
		cursor.execute("INSERT INTO reduced_submissions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", s)
	db.commit()
	if n % 10 == 0:
		print(str(n) + " subreddits filtered")

cursor.close()
db.commit()
db.close()
print("done!")
print(str(n) + " subreddits filtered")
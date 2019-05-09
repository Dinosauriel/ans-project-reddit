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

cursor = db.cursor(buffered=True)
cursor.execute("DROP TABLE IF EXISTS mobilizations")
cursor.execute("CREATE TABLE mobilizations ( \
	src_subreddit_name VARCHAR(15), \
	src_post_name VARCHAR(15) PRIMARY KEY, \
	src_post_author VARCHAR(15), \
	tgt_subreddit_name VARCHAR(15), \
	tgt_post_name VARCHAR(15), \
	tgt_post_author VARCHAR(15), \
	n_comments_by_src_memb_before INT, \
	n_comments_by_src_memb_after INT, \
	before_after_ratio DOUBLE, \
	INDEX(src_subreddit_name), \
	INDEX(src_post_author), \
	INDEX(tgt_subreddit_name), \
	INDEX(tgt_post_name), \
	INDEX(tgt_post_author) \
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

db.commit()

#creates table _tmp_members with members of subbreddit_a
def create_tmp_members(subreddit_a_name, subreddit_b_name, date, cursor):
	cursor.execute("DROP TABLE IF EXISTS _tmp_members")
	cursor.execute('''
		CREATE TABLE _tmp_members (author_fullname VARCHAR(15) PRIMARY KEY) AS
		SELECT DISTINCT author_fullname
		FROM comments c
		WHERE c.author_fullname IS NOT NULL
		AND c.subreddit_id = "''' + subreddit_a_name + '''"
		AND c.created_utc < ''' + str(date) + '''
		AND c.created_utc >= ''' + str(date - (3600 * 24 * 30)) + '''
		''')

		#AND c.author_fullname NOT IN
		#	(SELECT DISTINCT author_fullname
		#	FROM comments c
		#	WHERE c.subreddit_id = "''' + subreddit_b_name + '''"
		#	AND c.created_utc < ''' + str(date) + '''
		#	AND c.created_utc >= ''' + str(date - (3600 * 24 * 30)) + ''')

def create_tmp_comments_before(submission_name, date, cursor):
	cursor.execute("DROP TABLE IF EXISTS _tmp_comments_before")
	cursor.execute('''
		CREATE TABLE _tmp_comments_before (author_fullname VARCHAR(15), INDEX(author_fullname)) AS
		SELECT author_fullname
		FROM comments
		WHERE link_id = "''' + submission_name + '''"
		AND created_utc < ''' + str(date) + '''
		AND created_utc >= ''' + str(date - (3600 * 12)) + '''
		AND author_fullname IS NOT NULL
		''')

def create_tmp_comments_after(submission_name, date, cursor):
	cursor.execute("DROP TABLE IF EXISTS _tmp_comments_after")
	cursor.execute('''
		CREATE TABLE _tmp_comments_after (author_fullname VARCHAR(15), INDEX(author_fullname)) AS
		SELECT author_fullname
		FROM comments
		WHERE link_id = "''' + submission_name + '''"
		AND created_utc < ''' + str(date + (3600 * 12)) + '''
		AND created_utc >= ''' + str(date) + '''
		AND author_fullname IS NOT NULL
		''')

print("fetching crossposts...")

cursor.execute("SELECT id, author_fullname, created_utc, subreddit_id, crosspost_parent, num_comments FROM reduced_submissions WHERE crosspost_parent IS NOT NULL")
crossposts = cursor.fetchall()

print("searching for mobilizations...")

n = 0
n_mob = 0
for src_submission in crossposts:
	(src_id, src_author, src_created, src_subreddit_name, tgt_name, src_num_comments) = src_submission
	n += 1
	#fetch the target post
	cursor.execute("SELECT id, subreddit_id, author_fullname, num_comments FROM submissions WHERE id = \"" + tgt_name[3:] + "\"")
	tgt_submission = cursor.fetchone()

	if tgt_submission is not None:
		(tgt_id, tgt_subreddit_name, tgt_author, tgt_num_comments) = tgt_submission

		if tgt_num_comments <= 100:
			print("target has comments <= 100, skipping!")
			continue
		print("+++++++++++++++++++++++++++++++")

		print("creating temporary tables...")
		create_tmp_members(src_subreddit_name, tgt_subreddit_name, src_created, cursor)

		cursor.execute("SELECT COUNT(*) FROM _tmp_members")
		print("src community has " + str(cursor.fetchone()) + " members")

		create_tmp_comments_before(tgt_name, src_created, cursor)
		create_tmp_comments_after(tgt_name, src_created, cursor)

		print("counting comments before by source members...")
		cursor.execute('''
			SELECT COUNT(*)
			FROM _tmp_comments_before AS com
			WHERE com.author_fullname IN (
				SELECT author_fullname FROM _tmp_members
			)
		''')
		n_comments_by_src_memb_before = cursor.fetchone()[0]

		print("counting comments after by source members...")
		cursor.execute('''
			SELECT COUNT(*)
			FROM _tmp_comments_after AS com
			WHERE com.author_fullname IN (
				SELECT author_fullname FROM _tmp_members
			)
		''')
		n_comments_by_src_memb_after = cursor.fetchone()[0]
		print(n_comments_by_src_memb_after)
		
		if (n_comments_by_src_memb_before > 0):
			before_after_ratio = (n_comments_by_src_memb_after + n_comments_by_src_memb_before) / n_comments_by_src_memb_before
		else:
			before_after_ratio = n_comments_by_src_memb_after

		cursor.execute("DROP TABLE _tmp_members")
		cursor.execute("DROP TABLE _tmp_comments_before")
		cursor.execute("DROP TABLE _tmp_comments_after")

		#fetch the subreddits for debugging
		cursor.execute("SELECT * FROM subreddits WHERE id = \"" + src_subreddit_name[3:] + "\"")
		_src_subreddit = cursor.fetchone()
		cursor.execute("SELECT * FROM subreddits WHERE id = \"" + tgt_subreddit_name[3:] + "\"")
		_tgt_subreddit = cursor.fetchone()

		print("SOURCE:")
		print("submission: " + str(src_submission))
		print("subreddit: " + str(_src_subreddit))

		print("TARGET:")
		print("submission: " + str(tgt_submission))
		print("subreddit: " + str(_tgt_subreddit))

		print("n_comments_by_src_memb_before: " + str(n_comments_by_src_memb_before))
		print("n_comments_by_src_memb_after: " + str(n_comments_by_src_memb_after))
		print("before_after_ratio: " + str(before_after_ratio))

		if before_after_ratio > 1.6:
			print("this is a mobilization!")
			n_mob += 1
			mobilization_data = (
				src_subreddit_name,
				"t3_" + src_id,
				src_author,
				tgt_subreddit_name,
				tgt_name,
				tgt_author,
				n_comments_by_src_memb_before,
				n_comments_by_src_memb_after,
				before_after_ratio)
			cursor.execute("INSERT INTO mobilizations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", mobilization_data)

	if n % 100000 == 0:
		print(str(n) + " crossposts analyzed, " + str(n_mob) +  " mobilizations found")
		db.commit()

cursor.close()
db.commit()
db.close()
print("done!")
print(str(n) + " crossposts analyzed, " + str(n_mob) +  " mobilizations found")

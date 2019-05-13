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

cursor.execute("SELECT * FROM mobilizations ORDER BY n_comments_by_src_memb_after DESC")
mobilizations = cursor.fetchall()

for mobilization in mobilizations:
	(src_subreddit_name, src_post_name, src_post_author, tgt_subreddit_name, tgt_post_name, tgt_post_author, n_comments_by_src_memb_before, n_comments_by_src_memb_after, before_after_ratio) = mobilization
	cursor.execute("SELECT display_name, subscribers FROM subreddits WHERE id = \"" + src_subreddit_name[3:] + "\"")
	(src_subreddit_display_name, src_subreddit_subscribers) = cursor.fetchone()

	cursor.execute("SELECT title, selftext FROM submissions WHERE id = \"" + src_post_name[3:] + "\"")
	(src_submission_title, src_submission_text) = cursor.fetchone()

	cursor.execute("SELECT display_name, subscribers FROM subreddits WHERE id = \"" + tgt_subreddit_name[3:] + "\"")
	(tgt_subreddit_display_name, tgt_subreddit_subscribers) = cursor.fetchone()

	cursor.execute("SELECT title, selftext FROM submissions WHERE id = \"" + tgt_post_name[3:] + "\"")
	(tgt_submission_title, tgt_submission_text) = cursor.fetchone()

	print("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	print("mobilization")
	print("comments_before: " + str(n_comments_by_src_memb_before))
	print("comments_after: " + str(n_comments_by_src_memb_after))
	print("ratio: " + str(before_after_ratio))
	print("")
	print("target post:")
	print("/r/" + tgt_subreddit_display_name + " with " + str(tgt_subreddit_subscribers) + " subscribers and id " + tgt_subreddit_name)
	print("id: " + tgt_post_name)
	print("title: " + tgt_submission_title)
	print("text: " + tgt_submission_text[0:50].replace("\n", " "))
	print("")
	print("source post:")
	print("/r/" + src_subreddit_display_name + " with " + str(src_subreddit_subscribers) + " subscribers and id " + src_subreddit_name)
	print("id: " + src_post_name)
	print("title: " + src_submission_title)
	print("text: " + src_submission_text[0:50].replace("\n", " "))


	input("press ENTER to continue")

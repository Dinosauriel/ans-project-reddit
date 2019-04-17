
import mysql.connector
import env_file

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

def execute_file(file):
	stmt = open(file).readlines()
	cursor.execute(stmt)

execute_file("./sql/create_crossposts_view.sql")
execute_file("./sql/create_comments_of_submission_function.sql")
execute_file("./sql/create_mobilizations_view.sql")

db.commit()
db.close()
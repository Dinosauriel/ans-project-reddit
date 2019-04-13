
import json
from multiprocessing.dummy import Pool as ThreadPool

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
#param task: array of comment jsons
#returns: concatenated string of processed comment jsons
def filter_task(task):
	result = ""
	for comment_json in task:
		comment = json.loads(comment_json)
		filtered_dict = {k:v for (k,v) in comment.items() if k in relevant_fields}
		result += json.dumps(filtered_dict) + "\n"
	return result

num_threads = 6

def filter_tasks(tasks):
	thread_pool = ThreadPool(num_threads)
	mapped = thread_pool.map(filter_task, tasks)
	thread_pool.close()
	thread_pool.join()
	return mapped

source_file = open("/Users/aurelfeer/Downloads/RS_2018-11.json")
out_file = open("/Users/aurelfeer/Downloads/prep_RS_2018-11.json", "w")

total_number_of_lines = 0
for line in source_file:
	total_number_of_lines += 1
print("there are " + str(total_number_of_lines) + " lines")

source_file.seek(0)

n_tasks = 20
#a task consits of task_size lines
task_size = 5_000


i = 0
j = 0
k = 0
task = []
tasks = []
for line in source_file:
	task.append(line)
	k += 1

	if k == task_size:
		tasks.append(task)
		i += 1
		k = 0
		task = []

	if i == n_tasks:
		results = filter_tasks(tasks)
		out_file.writelines(results)
		j += 1
		print("wrote " + str(j * n_tasks * task_size) + " lines: " + str(float(100 * j * n_tasks * task_size) / total_number_of_lines) + "%")
		i = 0
		tasks = []

#finish up remaining tasks
if i > 0:
	results = filter_tasks(tasks)
	out_file.writelines(results)
	print("wrote remainig " + str(i) + " lines")


source_file.close()
out_file.close()
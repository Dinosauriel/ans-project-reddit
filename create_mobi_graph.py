import mysql.connector
import env_file
import sys
from graph_tool.all import *

def create_tmp_members(subreddit_name, date, cursor):
	cursor.execute("DROP TABLE IF EXISTS _tmp_members")
	cursor.execute('''
		CREATE TABLE _tmp_members (author_fullname VARCHAR(15) PRIMARY KEY) AS
		SELECT DISTINCT author_fullname
		FROM comments c
		WHERE c.author_fullname IS NOT NULL
		AND c.subreddit_id = "''' + subreddit_name + '''"
		AND c.created_utc < ''' + str(date) + '''
		AND c.created_utc >= ''' + str(date - (3600 * 24 * 30)) + '''
		''')

env = env_file.get()

db = mysql.connector.connect(
	host=env["MYSQL_HOST"],
	user=env["MYSQL_USER"],
	password=env["MYSQL_PW"],
	database=env["MYSQL_DB"],
)
db.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')
cursor = db.cursor()

src_post_name = sys.argv[1]

print("fetching mobilization...")

cursor.execute("SELECT * FROM mobilizations WHERE src_post_name = \"" + src_post_name + "\"")
mobilization = cursor.fetchone()

if mobilization is None:
	print("mobilization not found")
	exit()

(src_subreddit_name, src_post_name, src_post_author, tgt_subreddit_name, tgt_post_name, tgt_post_author, n_comments_by_src_memb_before, n_comments_by_src_memb_after, before_after_ratio) = mobilization

cursor.execute("SELECT created_utc FROM submissions WHERE id = \"" + src_post_name[3:] + "\"")
mobilization_date = cursor.fetchone()[0]

print("evaluating members...")
create_tmp_members(src_subreddit_name, mobilization_date, cursor)

print("fetching comments on target post...")
cursor.execute("SELECT * FROM comments WHERE link_id = \"" + tgt_post_name + "\" ORDER BY created_utc ASC")
comments_on_target_post = cursor.fetchall()

print("found " + str(len(comments_on_target_post)) + " comments")


print("creating graph...")
graph = Graph(directed=True)

vertex_colors = graph.new_vertex_property("vector<double>")
vertex_visible = graph.new_vertex_property("bool")

graph.vertex_properties["colors"] = vertex_colors
graph.vertex_properties["visible"] = vertex_visible


c_to_v = {}

for comment in comments_on_target_post:
	(comment_id, comment_author, comment_author_fullname, comment_author_created_utc, comment_created_utc, comment_subreddit_id, comment_body, comment_link_id, comment_parent_id) = comment
	v = graph.add_vertex()
	c_to_v[comment_id] = int(v)

	#assign a color
	vertex_colors[v] = (0,1,0,1)
	if comment_author_fullname is not None:
		cursor.execute("SELECT COUNT(*) FROM _tmp_members WHERE author_fullname = \"" + comment_author_fullname + "\"")
		is_attacker_comment = cursor.fetchone()[0]
		if is_attacker_comment > 0:
			vertex_colors[v] = (1,0,0,1)

	#find parent comment if possible
	if comment_parent_id[0:3] == "t1_":
		#this is a comment to a comment
		parent_vertex_id = c_to_v[comment_parent_id[3:]]
		parent_vertex = graph.vertex(parent_vertex_id)
		graph.add_edge(v, parent_vertex)

#hiding all the isolated nodes with no neighbours
#for v in graph.vertices():
#	if v.in_degree() + v.out_degree() == 0:
#		vertex_visible[v] = False
#	else:
#		vertex_visible[v] = True
#graph.set_vertex_filter(graph.vertex_properties["visible"])

#largest connected subgraph
graph.set_vertex_filter(label_largest_component(graph, directed = False))

print("visualizing graph...")
pos = sfdp_layout(graph)
#pos = fruchterman_reingold_layout(graph, n_iter=500)
#pos = arf_layout(graph)
graph_draw(graph,
			vertex_color = graph.vertex_properties["colors"],
			vertex_fill_color = graph.vertex_properties["colors"],
			vertex_size = 5,
			nodesfirst = True,
			pos = pos,
			output = "mobilization_graph_" + src_post_name + ".pdf")
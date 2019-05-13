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

print("fetching comments on target post...")
cursor.execute("SELECT DISTINCT author_fullname FROM comments WHERE link_id = \"" + tgt_post_name + "\" AND author_fullname IS NOT NULL")
authors = cursor.fetchall()

n_authors = len(authors)
print("found " + str(len(comments_on_target_post)) + " comments by " + str(n_authors) + " authors")

cursor.execute("SELECT COUNT(*) FROM ( \
					SELECT DISTINCT author_fullname \
					FROM comments \
					WHERE link_id = \"" + tgt_post_name + "\" \
					AND author_fullname IS NOT NULL \
					AND author_fullname IN ( \
						SELECT author_fullname FROM _tmp_members \
					) \
				) AS attackers")
n_attackers = cursor.fetchone()[0]
n_defenders = n_authors - n_attackers
print("there are " + str(n_defenders) + " defenders and " + str(n_attackers) + " attackers")

print("creating graph...")
graph = Graph(directed=True)

vertex_colors = graph.new_vertex_property("vector<double>")
vertex_visible = graph.new_vertex_property("bool")
vertex_pm_attackers = graph.new_vertex_property("double")
vertex_pm_defenders = graph.new_vertex_property("double")

edge_pm_weights = graph.new_edge_property("int")

graph.vertex_properties["colors"] = vertex_colors
graph.vertex_properties["visible"] = vertex_visible
graph.vertex_properties["attackers"] = vertex_pm_attackers
graph.vertex_properties["defenders"] = vertex_pm_defenders

graph.edge_properties["weights"] = edge_pm_weights

#add authors as vertices
au_to_v = {}

for author in authors:
	author = author[0]
	v = graph.add_vertex()
	au_to_v[author] = int(v)

	#assign a color
	cursor.execute("SELECT COUNT(*) FROM _tmp_members WHERE author_fullname = \"" + author + "\"")
	is_attacker = cursor.fetchone()[0] > 0
	if is_attacker:
		vertex_pm_attackers[v] = 1.0 / n_attackers
		vertex_pm_defenders[v] = 0.0
		vertex_colors[v] = (1,0,0,1)
	else:
		vertex_pm_attackers[v] = 0.0
		vertex_pm_defenders[v] = 1.0 / n_defenders
		vertex_colors[v] = (0,1,0,1)

for comment in comments_on_target_post:
	(comment_id, comment_author, comment_author_fullname, comment_author_created_utc, comment_created_utc, comment_subreddit_id, comment_body, comment_link_id, comment_parent_id) = comment

	if comment_author_fullname is None:
		continue

	#find parent comment if possible and add an edge between authors
	if comment_parent_id[0:3] == "t1_":
		#this is a comment to a comment
		cursor.execute("SELECT author_fullname FROM comments WHERE id = \"" + comment_parent_id[3:] + "\"")
		parent_author_fullname = cursor.fetchone()[0]

		if parent_author_fullname is None:
			continue

		#add the edge and its weights
		comment_vertex = graph.vertex(au_to_v[comment_author_fullname])
		parent_vertex = graph.vertex(au_to_v[parent_author_fullname])
		e = graph.edge(comment_vertex, parent_vertex)
		if e is None:
			e = graph.add_edge(comment_vertex, parent_vertex)
			edge_pm_weights[e] = 1
		else:
			edge_pm_weights[e] += 1


print("calculating page ranks...")

a_page_rank = pagerank(graph, damping=0.75, pers=vertex_pm_attackers, weight=edge_pm_weights)
d_page_rank = pagerank(graph, damping=0.75, pers=vertex_pm_defenders, weight=edge_pm_weights)

mean_defenders_a_pr = 0
mean_attackers_d_pr = 0

for v in graph.vertices():
	if vertex_pm_defenders[v] == 0:
		#this is a attacker
		mean_attackers_d_pr += d_page_rank[v]
	else:
		#this is a defender
		mean_defenders_a_pr += a_page_rank[v]

mean_attackers_d_pr /= n_attackers
mean_defenders_a_pr /= n_defenders

print("mean Attackers D-Page-Rank: " + str(mean_attackers_d_pr))
print("mean Defenders A-Page-Rank: " + str(mean_defenders_a_pr))

#hiding all the isolated nodes with no neighbours
for v in graph.vertices():
	if v.in_degree() + v.out_degree() == 0:
		vertex_visible[v] = False
	else:
		vertex_visible[v] = True
graph.set_vertex_filter(graph.vertex_properties["visible"])




#largest connected subgraph
#graph.set_vertex_filter(label_largest_component(graph, directed = False))

print("visualizing graph...")
pos = sfdp_layout(graph, gamma = 4, theta = 1.0)
#pos = fruchterman_reingold_layout(graph, n_iter=500)
#pos = arf_layout(graph)
graph_draw(graph,
			vertex_color = graph.vertex_properties["colors"],
			vertex_fill_color = graph.vertex_properties["colors"],
			vertex_size = 5,
			nodesfirst = True,
			pos = pos,
			output = "mobilization_person_graph_" + src_post_name + ".pdf")
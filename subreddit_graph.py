import mysql.connector
import env_file
from graph_tool.all import *

env = env_file.get()

db = mysql.connector.connect(
	host=env["MYSQL_HOST"],
	user=env["MYSQL_USER"],
	password=env["MYSQL_PW"],
	database=env["MYSQL_DB"],
)
db.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')
cursor = db.cursor()

cursor.execute("SELECT DISTINCT src_subreddit_name FROM mobilizations \
				UNION \
				SELECT DISTINCT tgt_subreddit_name FROM mobilizations")
subreddits = cursor.fetchall()

print("found " +str(len(subreddits)) + " subreddits")

graph = Graph(directed=True)
sub_to_v = {}
for subreddit in subreddits:
	subreddit_name = subreddit[0]
	v = graph.add_vertex()
	sub_to_v[subreddit_name] = int(v)

print("fetching mobilizations...")

cursor.execute("SELECT * FROM mobilizations")
mobilizations = cursor.fetchall()

edge_weights = graph.new_edge_property("int")
graph.edge_properties["weights"] = edge_weights


for mobilization in mobilizations:
	(src_subreddit_name, src_post_name, src_post_author, tgt_subreddit_name, tgt_post_name, tgt_post_author, n_comments_by_src_memb_before, n_comments_by_src_memb_after, before_after_ratio) = mobilization

	src_vertex = graph.vertex(sub_to_v[src_subreddit_name])
	tgt_vertex = graph.vertex(sub_to_v[tgt_subreddit_name])

	e = graph.edge(src_vertex, tgt_vertex)
	if e is None:
		e = graph.add_edge(src_vertex, tgt_vertex)
		edge_weights[e] = 1
	else:
		edge_weights[e] += 1

#vertex_colors = graph.new_vertex_property("vector<double>")
vertex_visible = graph.new_vertex_property("bool")

#graph.vertex_properties["colors"] = vertex_colors
graph.vertex_properties["visible"] = vertex_visible

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
#pos = sfdp_layout(graph)
#pos = fruchterman_reingold_layout(graph, n_iter=500)
pos = arf_layout(graph, max_iter=0)
graph_draw(graph,
			#vertex_color = graph.vertex_properties["colors"],
			#vertex_fill_color = graph.vertex_properties["colors"],
			vertex_size = 5,
			#nodesfirst = True,
			pos = pos,
			output = "subreddit_graph.pdf")
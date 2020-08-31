from py2neo import Graph
import pandas as pd
# Change port as needed and add your password
graph = Graph("bolt://localhost:11003", auth=("neo4j", "*********"))

it_pgm_graph = ("""CALL gds.graph.create.cypher(
	'IT_Program_Knowledge',
	'MATCH (n) WHERE (n:Occupation) OR (n:Knowledge) RETURN id(n) AS id',
	'MATCH (opm:OPMSeries)-[r:IN_OPM_Series]-(occ:Occupation) WHERE opm.series IN ["GS-2210-0", "GS-0080-0", "GS-0340-0", "GS-0343-0"] WITH occ, opm MATCH (occ)-[f:Found_In]-(s:Knowledge) WHERE f.scale = "IM" AND f.datavalue > 2.99 RETURN id(occ) AS source, id(s) AS target'
)""")

it_pgm_results = ("""CALL gds.nodeSimilarity.stream('IT_Program_Knowledge')
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).title AS Occupation1, gds.util.asNode(node2).title AS Occupation2, similarity
ORDER BY Occupation1""")

task_occupation = (""" MATCH (task:Task)-[r:Found_In]-(o:Occupation)
 WHERE r.datavalue > 3  AND r.scale = "IM"
 WITH {item:id(o), categories: collect(id(task))} AS userData
 WITH collect(userData) AS data
 CALL gds.alpha.similarity.overlap.stream({nodeProjection: '*', relationshipProjection: '*', data: data})
 YIELD item1, item2, count1, count2, intersection, similarity
 RETURN gds.util.asNode(item1).title AS from, gds.util.asNode(item2).title AS to,
        count1, count2, intersection, similarity
 ORDER BY similarity DESCENDING""")

basic_skills = ("""MATCH (opm:OPMSeries)-[r:IN_OPM_Series]-(occ:Occupation)
WHERE opm.series IN ["GS-2210", "GS-1550", "GS-854", "GS-855"]
WITH occ, opm
MATCH (occ)-[f:Found_In]-(s:Basic_Skills)
WHERE f.datavalue > 3.75
RETURN opm.series AS OPM,  occ.title AS Occupation, occ.onet_soc_code AS SOC, labels(s) AS Group,  s.description AS Element, f.datavalue AS Value""")

graph.run(it_pgm_graph)
results = graph.run(it_pgm_results).to_data_frame()
results.head(10)
manage_analysts = results.query('Occupation1=="management analysts"')
manage_analysts.to_csv('data/manage_analysts.csv', header=True)

task_results = graph.run(task_occupation).to_data_frame()
basic_skills_results = graph.run(basic_skills).to_data_frame()
basic_skills_results.to_csv('data/basic_skills.csv', header=True)
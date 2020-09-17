query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///TechnologySkills.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
MATCH (t:Technology_Skills {elementID: '5.F.1'})
SET m:Technology_Skills
REMOVE m:Commodity
MERGE (m)-[r:Sub_Element_Of]-(t)
MERGE (p:Tech_Skill_Product {title: line.Example})
ON CREATE SET p.hottech = line.`Hot Technology`
WITH o, m, p, line
MERGE (m)-[:Technology_Used_In]->(o)
MERGE (p)-[:Technology_Product]-(m)
",{batchSize:10000})""") #29370

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///TechnologySkills.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Technology_Skills {commodityID: toInteger(line.`Commodity Code`)})
MERGE (p:Tech_Skill_Product {title: line.Example})
ON CREATE SET p.hottech = line.`Hot Technology`
WITH o, m, p, line
MERGE (m)-[:Technology_Used_In]->(o)
MERGE (p)-[:Technology_Product]-(m)
",{batchSize:10000})""") #29370

# Tools
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///ToolsUsed.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
MATCH (t:Tools {elementID: '5.G.1'})
SET m:Tools
REMOVE m:Commodity
MERGE (m)-[r:Sub_Element_Of]-(t)
MERGE (p:Tool_Product {title: line.Example})
ON CREATE SET p.hottech = 'N'
WITH o, m, p, line
MERGE (m)-[:Tools_Used_In]->(o)
MERGE (p)-[:Tool_Product]-(m)
",{batchSize:10000})""") #42278

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///ToolsUsed.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Tools {commodityID: toInteger(line.`Commodity Code`)})
MERGE (p:Tool_Product {title: line.Example})
ON CREATE SET p.hottech = 'N'
WITH o, m, p,line
MERGE (m)-[:Tools_Used_In]->(o)
MERGE (p)-[:Tool_Product]-(m)
",{batchSize:10000})""") #42278
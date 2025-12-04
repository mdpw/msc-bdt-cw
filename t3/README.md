## Create Neo4j set up in docker
docker run -d --name neo4j-graphdb -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/test1234 neo4j:latest

## Implementing a social network by creating a small dataset with Users and Relationships
# Execute script in create-dataset.txt
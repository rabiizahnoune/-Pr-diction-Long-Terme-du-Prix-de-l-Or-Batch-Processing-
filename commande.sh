
#Récupérer l'adresse IP du conteneur PostgreSQL
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres_airflow
#entrer dans le contenaire de postgres
docker exec -it postgres_airflow bash


#creer un role 
docker exec -it file_rouge-sleek-airflow-1 airflow users create --username admin --firstname Admin --lastname User --email admin@example.com --role Admin --password rajarabii1



#Récupérer l'adresse IP du conteneur PostgreSQL
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres_airflow
#entrer dans le contenaire de postgres
docker exec -it postgres_airflow bash

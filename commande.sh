
#Récupérer l'adresse IP du conteneur PostgreSQL
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres_airflow

# Package the project (initially needed for airflow only)
package:
	uv build --clear --wheel

# Build image from Dockerfile extending existing Airflow
docker-build:
	docker compose build

# Starts Airflow docker container
docker-up:
	docker compose up -d

# Stops Airflow docker container, removes containers
docker-down:
	docker compose down
# Variables
COMPOSE_FILE = docker-compose.yml
PROJECT_NAME = velib_project
ENV_FILE = .env

# Build docker images
build:
	docker compose -f $(COMPOSE_FILE) build

# Launch services in detached mode
up:
	docker compose -f $(COMPOSE_FILE) up -d

# Launch services with logs for debugging
up-logs:
	docker compose -f $(COMPOSE_FILE) up

# Stop services
down:
	docker compose -f $(COMPOSE_FILE) down

# Show logs of all services
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Cleanse all containers, volumes, and images
clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi all
	docker system prune -f

# First launch: build and up, with instructions
first-launch:
	@echo "Initial config..."
	@if [ ! -f $(ENV_FILE) ]; then \
		echo ".env make file..."; \
		echo "POSTGRES_USER=velib" > $(ENV_FILE); \
		echo "POSTGRES_PASSWORD=velib" >> $(ENV_FILE); \
		echo "POSTGRES_DB=velib_dw" >> $(ENV_FILE); \
		echo "JUPYTER_TOKEN=velibexplo" >> $(ENV_FILE); \
		echo "AIRFLOW_ADMIN_USERNAME=admin" >> $(ENV_FILE); \
		echo "AIRFLOW_ADMIN_PASSWORD=admin" >> $(ENV_FILE); \
		echo "AIRFLOW_FERNET_KEY=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")" >> $(ENV_FILE); \
	else \
		echo ".env already exist."; \
	fi
	@echo "data_lake's permissions correction..."
	@if command -v sudo >/dev/null 2>&1; then \
		sudo chown -R 50000:0 data_lake 2>/dev/null || echo "Permissions non corrected (sudo required or folder don't exsits)."; \
	else \
		chown -R 50000:0 data_lake 2>/dev/null || echo "Permissions non corrected (chown required)."; \
	fi
	@echo "Build and launch services..."
	$(MAKE) build
	$(MAKE) up
	@echo "First launch finished. Access the following services:"
	@echo "- Jupyter : http://localhost:8888 (token: velibexplo)"
	@echo "- Airflow : http://localhost:8081"
	@echo "- Spark : http://localhost:8080"
	@echo "- PostgreSQL : localhost:5432"

# Check status of containers
status:
	docker compose -f $(COMPOSE_FILE) ps

# Restart specific service (ex: make restart SERVICE=airflow-webserver)
restart:
	docker compose -f $(COMPOSE_FILE) restart $(SERVICE)

# Bash acces to a container (ex: make shell SERVICE=postgres)
shell:
	docker compose -f $(COMPOSE_FILE) exec $(SERVICE) bash

# help
help:
	@echo "Available targets:"
	@echo "  build       : Build the images"
	@echo "  up          : Start the services"
	@echo "  up-logs     : Start with logs"
	@echo "  down        : Stop the services"
	@echo "  logs        : Show logs"
	@echo "  clean       : Clean everything"
	@echo "  first-launch: First launch (build + up)"
	@echo "  status      : Container status"
	@echo "  restart     : Restart a service (make restart SERVICE=<name>)"
	@echo "  shell       : Access a container (make shell SERVICE=<name>)"
	@echo "  help        : This help"
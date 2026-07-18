.PHONY: up down ci

up:
	docker-compose up -d --build

down:
	docker-compose down -v

ci:
	@echo "Executando verificações de CI..."
	docker-compose ps

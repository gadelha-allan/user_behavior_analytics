.PHONY: up down ci

up:
	docker-compose up -d --build

down:
	docker-compose down -v

ci:
	@echo "Executando testes locais..."
	pip install pytest
	pytest test_dag.py

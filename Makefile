COMPOSE := docker compose -f infrastructure/docker-compose.yml

.PHONY: env up up-stocks down clean ps logs test dag-test

env:            ## buat infrastructure/.env dari template (sekali saja)
	@test -f infrastructure/.env || cp infrastructure/.env.example infrastructure/.env
	@echo "Edit infrastructure/.env lalu jalankan: make up"

up:             ## jalankan seluruh stack
	$(COMPOSE) up -d --build

up-stocks:      ## + producer saham US (butuh FINNHUB_API_KEY)
	$(COMPOSE) --profile stocks up -d --build

down:           ## stop (data tetap ada)
	$(COMPOSE) --profile stocks down

clean:          ## stop + hapus semua data (Kafka & PostgreSQL)
	$(COMPOSE) --profile stocks down -v

ps:
	$(COMPOSE) ps

logs:           ## contoh: make logs s=processor
	$(COMPOSE) logs -f --tail 50 $(s)

test:           ## unit test
	python -m pytest --cov=processing.transform --cov=processing.aggregate --cov-report=term

dag-test:       ## jalankan DAG sekali untuk jam berjalan (logical date = jam berikutnya)
	$(COMPOSE) exec airflow bash -c 'airflow dags test market_batch_layer "$$(date -u -d "+1 hour" +%Y-%m-%dT%H:00:00+00:00)"'

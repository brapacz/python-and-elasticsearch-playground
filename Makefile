setup:
	pip3 install -r requirements.txt
	docker-compose up -d
	@echo "It may take a moment for elasticsearch to become responsive!"

wait-for-elastic:
	@wget http://0.0.0.0:9200 -O /dev/null -o /dev/null --tries=60 --timeout=2 && echo "ok" || (echo "unresponsive"; exit 1)

test:
	python3 -m unittest *_test.py

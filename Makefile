install:
	pip3 install -r requirements.txt
	docker-compose up -d

test:
	python3 -m unittest *_test.py

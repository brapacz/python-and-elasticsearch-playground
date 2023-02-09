setup:
	pip3 install -r requirements.txt
	docker-compose up -d
	@echo "It may take a moment for elasticsearch to become responsive!"

test:
	python3 -m unittest *_test.py

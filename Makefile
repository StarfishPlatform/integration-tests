init:
	pip install -r requirements.txt

test:
	pytest tests

watch:
	ptw tests

.PHONY: init test

.PHONY: run demo test docker clean

run:        ## one command: start hub + demo partner receiver
	./run.sh

demo:       ## run the end-to-end scenario against the running hub
	python3 scripts/demo.py

test:       ## run the test suite
	python3 -m unittest discover -s tests -t . -v

docker:     ## containerized one-command start
	docker compose up --build

clean:
	rm -rf data

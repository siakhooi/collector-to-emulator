help:
clean:
	rm -rf dist target coverage .tox .coverage \
	src/collector_to_emulator/__pycache__ \
	templates/ templates123/ scenario.yaml s1.yaml s2.yaml build.log testings/templates/ testings/scenario.yaml \
	tests/__pycache__ .pytest_cache .venv
run: run-s1
run-1:
	poetry run collector-to-emulator testings/collector.jsonl
run-2:
	poetry run collector-to-emulator -i testings/collector.jsonl
run-3:
	poetry run collector-to-emulator < testings/collector.jsonl
run-t:
	poetry run collector-to-emulator -i testings/collector.jsonl -t templates123
run-s1:
	poetry run collector-to-emulator testings/collector.jsonl -s s1.yaml -n 'Scenario 123'
run-s2:
	poetry run collector-to-emulator < testings/collector.jsonl > s2.yaml
run-s1g:
	poetry run collector-to-emulator testings/collector.jsonl -s s1.yaml -n 'Scenario 123' -g 5000
run-s1c:
	poetry run collector-to-emulator testings/collector.jsonl -s s1.yaml -n 'Scenario 123' -c 2000
set-version:
	scripts/set-version.sh
build:
	poetry build
install:
	poetry install
flake8:
	poetry run flake8
update:
	poetry update
test:
	 poetry run pytest --capture=sys \
	 --junit-xml=coverage/test-results.xml \
	 --cov=collector_to_emulator \
	 --cov-report term-missing  \
	 --cov-report xml:coverage/coverage.xml \
	 --cov-report html:coverage/coverage.html \
	 --cov-report lcov:coverage/coverage.info

all: clean set-version install flake8 build tox-run
one: clean set-version install flake8 build
	tox run -e py314

release:
	scripts/release.sh

fix-cert:
	pip install pip-system-certs --trusted-host pypi.org --trusted-host files.pythonhosted.org
fix-pyenv:
	 pyenv versions --bare > .python-version
tox-run:
	tox run

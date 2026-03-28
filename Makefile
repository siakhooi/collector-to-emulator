help:
clean:
	rm -rf dist target coverage .tox .coverage \
	src/collector_to_mocker/__pycache__ \
	tests/__pycache__
run:
	poetry run collector-to-mocker
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
	 --cov=collector_to_mocker \
	 --cov-report term-missing  \
	 --cov-report xml:coverage/coverage.xml \
	 --cov-report html:coverage/coverage.html \
	 --cov-report lcov:coverage/coverage.info

all: clean set-version install flake8 build tox-run

release:
	scripts/release.sh

fix-cert:
	pip install pip-system-certs --trusted-host pypi.org --trusted-host files.pythonhosted.org
fix-pyenv:
	 pyenv versions --bare > .python-version
tox-run:
	tox run

.DEFAULT_GOAL := lint

lint-tests:
	pylint tests -d broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments,too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,wrong-spelling-in-comment,wrong-spelling-in-docstring,duplicate-code,no-name-in-module,import-error,consider-using-f-string

lint-code:
	pylint dz_dynamodb -d broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments,too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,wrong-spelling-in-comment,wrong-spelling-in-docstring,raise-missing-from,consider-using-f-string

lint: lint-code lint-tests

build-tap:
	rm -rf dist;
	python setup.py sdist bdist_wheel

build-tap-local:
	rm -rf dist ;
	pip uninstall wheel;
	pip uninstall dz-dynamodb;
	pip install wheel;
	python setup.py sdist bdist_wheel;
	pip install --no-index -f ./dist dz-dynamodb

publish:
	python -m twine upload dist/*

# run-read-tap:
#  	python dz_dynamodb/__init__.py  --config ./creds/config.json --catalog ./creds/catalog.json --state ./creds/state.json

run-discover-tap:
	python dz_dynamodb/__init__.py --discover  --config ./creds/config.json > ./creds/catalog.json

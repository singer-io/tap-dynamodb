.DEFAULT_GOAL := lint

lint-tests:
	pylint tests -d broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments,too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,wrong-spelling-in-comment,wrong-spelling-in-docstring,duplicate-code,no-name-in-module,import-error,consider-using-f-string

lint-code:
	pylint tap_dz_dynamodb -d broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments,too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,wrong-spelling-in-comment,wrong-spelling-in-docstring,raise-missing-from,consider-using-f-string

lint: lint-code lint-tests

build-tap:
	python setup.py sdist bdist_wheel

publish:
	python -m twine upload dist/*
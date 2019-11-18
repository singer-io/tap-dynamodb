.DEFAULT_GOAL := test

test:
	pylint tap_dynamodb -d missing-docstring,fixme,duplicate-code,line-too-long,invalid-name,too-few-public-methods

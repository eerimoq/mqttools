test:
	python3.7 setup.py test

release-to-pypi:
	python3.7 setup.py sdist
	python3.7 setup.py bdist_wheel --universal
	twine upload dist/*

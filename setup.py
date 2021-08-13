from distutils.core import setup

majv = 1
minv = 1

setup(
	name = 'pymtar',
	version = "%d.%d" %(majv,minv),
	description = "Python module to store magnetic tape tar data in a sqlite database",
	author = "Colin ML Burnett",
	author_email = "cmlburnett@gmail.com",
	url = "https://github.com/cmlburnett/pymtar",
	packages = ['pymtar'],
	package_data = {'pymtar': ['pymtar/__init__.py', 'pymtar/__main__.py']},
	classifiers = [
		'Programming Language :: Python :: 3.9'
	]
)

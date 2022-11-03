from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in payroll_lavado/__init__.py
from payroll_lavado import __version__ as version

setup(
	name="payroll_lavado",
	version=version,
	description="Define & APlly Payroll Penalties",
	author="Lavaloon",
	author_email="sales@lavaloon.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)

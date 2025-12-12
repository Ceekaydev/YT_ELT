from setuptools import setup, find_packages

setup(
    name="my_yt_utils",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "apache-airflow>=3.0.0"
    ],
)

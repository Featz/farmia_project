# setup.py
from setuptools import setup, find_packages

setup(
    name="farmia_engine",
    version="0.1.0",
    author="Fernando Regodeceves",
    description="Motor de ingesta de datos para FarmIA",
    long_description="Un motor de ingesta de datos que procesa archivos y los mueve a través de las capas de un data lakehouse (landing, raw, bronze).",
    packages=find_packages(where="."), 
    install_requires=[
        "pyspark>=3.5.0", 
    ],
    python_requires='>=3.10, <=3.11.11', # Especifica versiones de Python compatibles
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
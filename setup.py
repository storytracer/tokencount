from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="tokencount",
    version="0.1.0",
    author="Sebastian Majstorovic",
    description="A tool to count tokens in datasets using tiktoken",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/storytracer/tokencount",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    py_modules=["tokencount"],
    entry_points={
        "console_scripts": [
            "tokencount=tokencount:cli",
        ],
    },
)
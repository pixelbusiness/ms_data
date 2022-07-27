import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='ms_database',
    version='0.1',
    scripts=[],
    author="pixelbusiness",
    author_email="rumgepixelt@gmail.com",
    description="stupid database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pixelbusiness/ms_data",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"

    ],
)

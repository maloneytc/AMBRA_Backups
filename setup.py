import setuptools

with open("README.md", "r") as fopen:
    long_description = fopen.read()

setuptools.setup(
    name="AMBRA_Backups",
    version="0.1",
    description="Various utility classes for connecting to the AMBRA API and backing up study data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    author="Thomas Maloney",
    author_email="malonetc@ucmail.uc.edu",
    license="MIT",
    packages=setuptools.find_packages(),
    zip_safe=False,
    python_requires=">=3.6",
)

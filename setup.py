from setuptools import setup, find_packages

setup(
    name="metatreedb",
    version="0.2.3",
    author="oboki",
    author_email="oboki@kakao.com",
    description="Metatree is a DBMS that uses the filesystem itself as a tree-structured database.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/oboki/metatreedb",
    packages=find_packages(),
    install_requires=[
        "fsspec",
        "hdfs",
        "pyyaml",
        "s3fs",
    ],
    extras_require={
        "dev": [
            "pytest",
            "wheel",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.7",
)

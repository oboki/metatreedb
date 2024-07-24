from setuptools import setup, find_packages

setup(
    name="metatreedb",
    version="0.1.2",
    author="oboki",
    author_email="oboki@kakao.com",
    description="File system-based, easy-to-read database",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/oboki/metatreedb",
    packages=find_packages(),
    install_requires=["pyyaml", "requests"],
    extras_require={"dev": ["pytest"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.8",
)

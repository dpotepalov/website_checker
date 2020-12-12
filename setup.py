import setuptools

with open("README.rst", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="website-checker",
    version="0.0.1",
    author="Dmitry Potepalov",
    author_email="dpotepalov@yandex.ru",
    description="A simple website availability checker",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/dpotepalov/website_checker",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3", "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent", "Development Status :: 3 - Alpha"
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'checker-producer=website_checker.producer:main',
            'checker-consumer=website_checker.consumer:main',
        ],
    },
)
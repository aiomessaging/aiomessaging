"""aiomessaging package.
"""
import setuptools
from distutils.core import setup


setup(
    name='aiomessaging',
    packages=['aiomessaging'],
    version='0.2',
    description='Python asyncio messaging Framework',
    author='Roman Tolkachyov',
    author_email='roman@tolkachyov.name',
    url='https://github.com/aiomessaging/aiomessaging/',
    download_url='https://github.com/aiomessaging/aiomessaging/archive/0.2.tar.gz',  # noqa
    keywords="asyncio, messaging, events, queues, email, delivery, sms, push",
    classifiers=[
        "Development Status :: 1 - Planning",

        "Framework :: AsyncIO",

        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",

        "Topic :: Internet",
        "Topic :: System :: Networking",
        "Topic :: Software Development :: Libraries",
        "Topic :: Communications :: Email",
        "Topic :: Communications :: Chat",
        "Topic :: Utilities",

        "License :: OSI Approved :: MIT License"
    ],
    install_requires=[
        'attrdict',
        'ujson',
        'pika',
        'PyYAML',
        'termcolor'
    ],
    extras_require={
        'dev': [
            'Sphinx',
            'commonmark'
        ]
    }
)

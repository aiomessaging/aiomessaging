from distutils.core import setup


setup(
    name='aiomessaging',
    packages=['aiomessaging'],
    version='0.2',
    description='Python asyncio messaging Framework',
    author='Roman Tolkachyov',
    author_email='roman@tolkachyov.name',
    url='https://github.com/romantolkachyov/aiomessaging/',
    download_url='https://github.com/romantolkachyov/aiomessaging/archive/0.2.tar.gz',  # noqa
    keywords="asyncio, messaging, events, queues, email, delivery, sms, push",
    classifiers=[],
    install_requires=open('requirements.txt').read().split('\n'),
    extras_require={
        'dev': [
            'Sphinx',
            'commonmark'
        ],
        'test': [
            'pytest',
            'pytest-catchlog',
            'pytest-cov',
            'pytest-watch'
        ],
    },
    entry_points={
        'console_scripts': [
            'pmf aiomessaging.cli:run',
        ],
    },
)

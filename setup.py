from setuptools import setup

setup(
    name='cogdb',
    version='4.0.0rc1',
    description='Persistent Embedded Graph Database',
    url='https://github.com/arun1729/cog',
    project_urls={
        'Homepage': 'https://cogdb.io',
        'Source': 'https://github.com/arun1729/cog',
        'Documentation': 'https://cogdb.io',
    },
    author='Arun Mahendra',
    author_email='hello@cogdb.io',
    license='MIT',
    packages=['cog'],
    install_requires=['xxhash>=3.2.0', 'simsimd>=5.0.0', 'websocket-client>=1.9.0', 'certifi'],
    extras_require={
        'dev': ['pytest', 'pytest-cov'],
    },
    python_requires='>=3.8',
    zip_safe=False,
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
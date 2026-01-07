from setuptools import setup

setup(
    name='cogdb',
    version='3.4.0',
    description='Persistent Embedded Graph Database',
    url='http://github.com/arun1729/cog',
    author='Arun Mahendra',
    author_email='arunm3.141@gmail.com',
    license='MIT',
    packages=['cog'],
    install_requires=['xxhash==3.2.0', 'simsimd>=5.0.0'],
    extras_require={
        'dev': ['pytest', 'pytest-cov'],
    },
    python_requires='>=3.8',
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)

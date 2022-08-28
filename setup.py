from setuptools import setup


setup(name='cogdb',
      version='3.0.2',
      description='Persistent Embedded Graph Database',
      url='http://github.com/arun1729/cog',
      author='Arun Mahendra',
      author_email='arunm3.141@gmail.com',
      license='MIT',
      packages=['cog'],
      install_requires=['xxhash==2.0.0'],
      zip_safe=False)

from setuptools import setup
from setuptools import find_packages

setup(name='chariots',
      version='0.1.0',
      description='machine learning versioned pipelines',
      url='https://github.com/aredier/chariots',
      author='Antoine Redier',
      author_email='antoine.redier@hec.edu',
      license='MIT',
      packages=['chariots'],
      extras_require={
          'tests': ['pytest',
                    'sure',
                    'numpy',
                    'sklearn',
                    'pandas',
                    'tensorflow',
                    'keras',
                   ],
      },
      zip_safe=False)
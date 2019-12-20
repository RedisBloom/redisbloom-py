
from setuptools import setup, find_packages
setup(
    name='redisbloom',
    version='0.3.0',

    description='RedisBloom Python Client',
    url='https://github.com/redislabs/redisbloom-py',
    packages=find_packages(),
    install_requires=['redis', 'hiredis'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database'
    ]
)

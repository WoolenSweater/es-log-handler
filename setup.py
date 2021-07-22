from setuptools import setup, find_packages

setup(
    name='es-log-handler',
    version='0.3.1',
    description='Elasticsearch Log Handler ',
    long_description=open('README.md', 'r').read(),
    long_description_content_type="text/markdown",
    author='Nikita Ryabinin',
    author_email='ryabinin.ne@gmail.com',
    install_requires=['elasticsearch'],
    url='https://github.com/WoolenSweater/es-log-handler',
    packages=find_packages(),
    python_requires='>=3.6'
)

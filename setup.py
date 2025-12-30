from setuptools import setup, find_packages

setup(
    name =                      'cosim',
    version =                   '0.0.1',
    author =                    'Nelson.S',
    author_email =              'mail.nelsonsharma@gmail.com',
    description =               'computation offloading simulator',
    packages =                  find_packages(include=['cosim', 'cosim.*']),
    classifiers=                ['License :: OSI Approved :: MIT License'],
    install_requires =          [],
    include_package_data =      False,
)   
from setuptools import setup

setup(
    name='prioritization_discovery',
    version='0.3.0',
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        'pandas',
        'pandasql',
        'scikit-learn'
    ],
)

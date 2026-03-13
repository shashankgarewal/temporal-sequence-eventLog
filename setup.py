from setuptools import find_packages, setup
from typing import List


def get_requirements(file_path: str) -> List[str]:
    """converts requirement.txt file content to list"""
    
    requirements=[]

    with open(file_path) as file_obj:
        requirements=file_obj.readlines()
        requirements=[req.replace('\n',"") for req in requirements]

        try:
            requirements.remove('-e .')
        except:
            pass

    return requirements

setup(
    name                = 'eventlogs',
    version             = '0.1.0',
    author              = 'shashankgarewal',
    author_email        = 'shashankgarewal4+github@gmail.com',
    packages            = find_packages(),
    install_requires    = get_requirements('requirements.txt')
)
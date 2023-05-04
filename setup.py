from setuptools import find_packages
from setuptools import setup

setup(
    name="jupyterhub-backendspawner",
    version="0.2.4",
    description="JupyterHub Spawner to spawn on different systems.",
    url="https://github.com/kreuzert/jupyterhub-backendspawner",
    author="Tim Kreuzer",
    author_email="t.kreuzer@fz-juelich.de",
    license="3-BSD",
    packages=find_packages(),
    install_requires=["jupyterhub","traitlets"],
    python_requires=">=3.9",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)

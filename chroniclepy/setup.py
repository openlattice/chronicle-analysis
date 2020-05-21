from setuptools import setup, find_packages

setup(name='chroniclepy',
      version='1.5',
      description='Package for preprocessing Chronicle data.',
      author='OpenLattice',
      author_email='info@openlattice.com',
      license='GNU GPL v3.0',
      install_requires=[
          'pandas>0.15.0',
          ],
      packages = find_packages(),
      zip_safe=False)

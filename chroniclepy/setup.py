from setuptools import setup

setup(name='chroniclepy',
      version='1.2-rc3',
      description='Package for preprocessing Chronicle data.',
      author='OpenLattice',
      author_email='info@openlattice.com',
      license='GNU GPL v3.0',
      packages=['chroniclepy'],
      install_required=[
          'pandas>0.15.0',
          ],
      package_dir={'chroniclepy':'src'},
      zip_safe=False)

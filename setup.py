import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*.py") 

# The main call
setup(name='desthumbs',
      version ='0.1.0',
      license = "GPL",
      description = "A python module to make FITS files cutouts/thumbnails for DES",
      author = "Felipe Menanteau",
      author_email = "felipe@illinois.edu",
      packages = ['desthumbs'],
      package_dir = {'': 'python'},
      scripts =  ['bin/makeDESthumbs'],
      data_files=[('ups',['ups/desthumbs.table']),
                  ('etc',  glob.glob("etc/*.*")),
                  ],
      )


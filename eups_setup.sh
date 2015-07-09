#!/usr/bin/env bash

echo "Setting up EUPS for Thumbnails"
source $EUPS_DIR/desdm_eups_setup.sh

# pre-reqs
setup -r ~/DESDM-Code/devel/despyastro/trunk
setup despydb 2.0.0+2
setup fitsio 0.9.8rc1+0 

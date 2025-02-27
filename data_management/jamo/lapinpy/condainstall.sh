#!/bin/bash
package_dir=$1
    conda create python=2.7 -y -p $package_dir
    source activate $package_dir
    conda install -y appdirs
    #source deactivate $(package_dir)
    #source activate $(package_dir)
    #virtualenv --system-site-packages $(package_dir)
    # Temporary fix to get around the upgrade conflict for pbr
    #$(pip) install --upgrade pbr
    #pip install --upgrade appdirs
    pip install --upgrade packaging
    #pip install --upgrade setuptools
    pip install --upgrade -r requirements.txt
    #export PYTHONPATH=/global/homes/s/sdm/lapin_source/lapin_build/lib/python2.7/site-packages:/global/homes/s/sdm/lapin_source/lapin_build/bin/python
    export PYTHONPATH=`pwd`/$package_dir/bin/python
    python setup.py clean --all
    python setup.py install

#!/bin/bash
package_dir=$1
    conda create python=3.9 -y -p $package_dir
    conda activate $package_dir
    conda install -y appdirs
    pip install packaging
    pip install setuptools==58
    pip install --upgrade -r requirements_py3.txt
    python setup_py3.py clean --all
    python setup_py3.py install
    # Fix for python-daemon lib in Python 3
    sed -i -e "s/app.stderr_path, 'w+t', buffering=0/app.stderr_path, 'w+t'/" $package_dir/lib/python*/site-packages/daemon/runner.py

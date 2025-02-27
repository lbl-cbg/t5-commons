# Installation

The following commands will install the code in this repository in such a way
that will allow one to use the tools provided by said code. With that said, 
the provided sequence of commands may not suit your specific needs.
As this repository follows PEP 517 style packaging, there are many 
ways to install the software, so please use discretion and adapt as necessary.

```bash
git clone https://code.jgi.doe.gov/advanced-analysis/jgi-lapinpy.git
cd jgi-lapinpy
pip install -r requirements.txt
python setup.py install
```
# Commands

All commands available with the LapinPy package can be accessed using the `lapind` 
command

## Setting up LapinPy
 - `lapind init` - Set up core database needed for running LapinPy

## Running LapinPy
 - `lapind start` - Start a LapinPy daemon process
 - `lapind stop` - Stop a LapinPy daemon process
 - `lapind restart` - Restart a LapinPy daemon process
 - `lapind run` - Run a LapinPy process not as a daemon

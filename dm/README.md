# Running JAMO at NERSC

This directory contains all the necessary files for setting up JAMO at NERSC. Running JAMO at NERSC
is broken down into four parts:

- Building the JAMO application as a Docker image. The relevant files for this can be found in `jamo/docker`.
- Instantiating JAMO on NERSC's Spin infrastructure. The necessary files for this can be found in `jamo/k8s` and `jamo/config`.
- Running the data transfer service on Perlmutter and the Data transfer nodes. The necessary files for this can be found in `jamo/dt_service`.

See the wiki for more details on [setting up JAMO](https://github.com/lbl-cbg/t5-data/wiki/Setting-up-JAMO).

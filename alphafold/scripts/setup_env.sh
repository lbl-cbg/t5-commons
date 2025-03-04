#!/bin/bash -e

# type wget 2>/dev/null || { echo "wget is not installed. Please install it using apt or yum." ; exit 1 ; }
#
# CURRENTPATH=`pwd`
# COLABFOLDDIR="${CURRENTPATH}/localcolabfold"
#
# mkdir -p "${COLABFOLDDIR}"
# cd "${COLABFOLDDIR}"
# wget -q -P . https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
# bash ./Miniforge3-Linux-x86_64.sh -b -p "${COLABFOLDDIR}/conda"
# rm Miniforge3-Linux-x86_64.sh
#
# source "${COLABFOLDDIR}/conda/etc/profile.d/conda.sh"
# export PATH="${COLABFOLDDIR}/conda/condabin:${PATH}"
# conda update -n base conda -y
#

MSA_DB_DIR="$CFS/m4521/resources/colabfold/msa_db"

# Initialize variables
EDITABLE_FLAG=""

# Function to display usage information
usage() {
  echo "Usage: $0 [-e] [-h <msa_dir>] <user> <token>"
  echo "  -e              Install the common and alphafold code in editable mode"
  echo "  -m <msa_dir>    The path to the MSA database directory"
  echo "  <user>          The user account to connect to Jira with"
  echo "  <token>         The token to use to connect to Jira with"
  exit 1
}

# Parse command-line options
while getopts "ec:" opt; do
  case ${opt} in
    e )
      EDITABLE_FLAG="-e"
      ;;
    m )
      MSA_DB_DIR=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

# Shift the parsed options so that $1 and $2 are the positional arguments
shift $((OPTIND -1))

# Check if the required positional arguments are provided
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Error: Two required positional arguments must be provided."
  usage
fi

JIRA_USER=${1}
JIRA_TOKEN=${2}

module load python

COLABFOLDDIR='./'

conda create -p "$COLABFOLDDIR/colabfold-conda" -c conda-forge -c bioconda \
    git python=3.10 openmm==8.0.0 pdbfixer \
    kalign2=2.04 hhsuite=3.3.0 mmseqs2 -y
conda activate "$COLABFOLDDIR/colabfold-conda"

# install ColabFold and Jaxlib
"$COLABFOLDDIR/colabfold-conda/bin/pip" install --no-warn-conflicts \
    "colabfold[alphafold-minus-jax] @ git+https://github.com/sokrypton/ColabFold"
"$COLABFOLDDIR/colabfold-conda/bin/pip" install "colabfold[alphafold]"
"$COLABFOLDDIR/colabfold-conda/bin/pip" install --upgrade "jax[cuda12]"==0.4.35
"$COLABFOLDDIR/colabfold-conda/bin/pip" install --upgrade tensorflow
"$COLABFOLDDIR/colabfold-conda/bin/pip" install silence_tensorflow

# Download the updater
wget -qnc -O "$COLABFOLDDIR/update_linux.sh" \
    https://raw.githubusercontent.com/YoshitakaMo/localcolabfold/main/update_linux.sh
chmod +x "$COLABFOLDDIR/update_linux.sh"

pushd "${COLABFOLDDIR}/colabfold-conda/lib/python3.10/site-packages/colabfold"
# Use 'Agg' for non-GUI backend
sed -i -e "s#from matplotlib import pyplot as plt#import matplotlib\nmatplotlib.use('Agg')\nimport matplotlib.pyplot as plt#g" plot.py
# modify the default params directory
sed -i -e "s#appdirs.user_cache_dir(__package__ or \"colabfold\")#\"${COLABFOLDDIR}/colabfold\"#g" download.py
# suppress warnings related to tensorflow
sed -i -e "s#from io import StringIO#from io import StringIO\nfrom silence_tensorflow import silence_tensorflow\nsilence_tensorflow()#g" batch.py
# remove cache directory
rm -rf __pycache__
popd

# Download weights
"$COLABFOLDDIR/colabfold-conda/bin/python3" -m colabfold.download
mv colabfold af2_weights
echo "Download of alphafold2 weights finished."
echo "-----------------------------------------"
echo "Installation of ColabFold finished."
echo "Add ${COLABFOLDDIR}/colabfold-conda/bin to your PATH environment variable to run 'colabfold_batch'."
echo "Run 'conda activate $PWD/colabfold-conda' to activate"
echo "For more details, please run 'colabfold_batch --help'."

# Set environment variables needed for running the commands in this package
script_dir=`dirname $0`
pip install $EDITABLE_FLAG $script_dir/../../common
pip install $EDITABLE_FLAG $script_dir/../
conda env config vars set JIRA_USER=$JIRA_USER
conda env config vars set JIRA_TOKEN=$JIRA_TOKEN
conda env config vars set JIRA_HOST=https://taskforce5.atlassian.net
conda env config vars set MSA_DB_DIR=$MSA_DB_DIR
conda env config vars set AF2_WEIGHTS_DIR=$PWD/af2_weights

# This script is should be run as t5user on Perlmutter

cd `dirname $0`

module load conda

conda activate /global/common/software/m4521/t5wf
pip uninstall --yes t5common
pip install ./common

conda activate /global/cfs/cdirs/m4521/resources/colabfold/colabfold-conda
pip uninstall --yes t5common t5af
pip install ./common
pip install ./alphafold

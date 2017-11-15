# set -o errexit                  # equivalent to set -e
# set -o nounset                  # equivalent to set -u
# set -o pipefail                 # catch failure in pipe
set -o xtrace                   # equivalent to set -x

# make sure you're in a python virtual environment
# example usage: . gen-test-bam.sh in.bam "contig1 contig2"

# make sure pysam is in the environment'
source activate venv
python -c 'import pysam'

INPUT_BAM=$1
CONTIGS=$2

DIR=$(dirname ${INPUT_BAM})
OUTPUT_BAM=${DIR}/$(basename ${INPUT_BAM} .bam).test.bam
TMP_SAM=${DIR}/.$(basename ${INPUT_BAM} .bam).tmp.sam
TMP_SH_SAM=${DIR}/$(basename ${TMP_SAM}.sam)-sh.sam

echo "subsampling ${INPUT_BAM}"
samtools view -h ${INPUT_BAM} ${CONTIGS} > ${TMP_SAM}

echo "filter reference names in SAM header"
python filter-sam-header.py ${TMP_SAM} ${TMP_SH_SAM}

echo "convert SAM to BAM"
samtools view -b ${TMP_SAM_SH} > ${OUTPUT_BAM}

rm -fv ${TMP_SAM} ${TMP_SAM_SH}

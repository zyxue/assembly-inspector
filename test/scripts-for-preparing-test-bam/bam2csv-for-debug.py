import os
import sys

import pysam

import pandas as pd

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')

# infile_name = './E00455-A64450_links10000scaffolds_renamed-nameSorted_filtered_BXsorted.test.bam'
infile_name = sys.argv[1]
# outfile_name = 'out.sam'

logging.info('reading {0}'.format(infile_name))
infile = pysam.AlignmentFile(infile_name)
# outfile = pysam.AlignmentFile(outfile_name, 'w', template=infile)

out_pkl_prefix = os.path.join(
    os.path.dirname(infile_name),
    # '/dev/shm/',
    os.path.basename(infile_name).rstrip('.bam')
)


# target_contigs = set(['16444', '420042', '612565'])

logging.info('start looping')

cols = [
    'query_name', 'read_len',
    'reference_id',
    'reference_name',
    'reference_len',
    'reference_start', 'reference_end',
    # 'BX_tag',
    'mapping_quality',
    'query_alignment_start', 'query_alignment_end'
]

pkl_section = 0 
results = []
for k, rec in enumerate(infile):
    if (k + 1) % 1000000 == 0:
        logging.info('processed {0} records'.format(k + 1))

    if (
            rec.reference_id == -1
            or rec.mapping_quality == 0
            or rec.reference_end is None  # this could be a result of MAPQ=0
    ):
        continue

    results.append((
        rec.query_name,
        rec.infer_read_length(),
        rec.reference_id,
        rec.reference_name,
        rec.reference_length,
        rec.reference_start, rec.reference_end,
        # rec.get_tag('BX'),
        rec.mapping_quality,
        rec.query_alignment_start, rec.query_alignment_end
    ))

    if (k + 1) % 100000001 == 0:
        out_pkl = out_pkl_prefix + '.{0}.pkl'.format(pkl_section)
        logging.info('writing to {0}'.format(out_pkl))
        out_df = pd.DataFrame(results, columns=cols)
        out_df.to_pickle(out_pkl)
        pkl_section += 1
        results = []

out_pkl = out_pkl_prefix + '.{0}.pkl'.format(pkl_section)
logging.info('writing to {0}'.format(out_pkl))
out_df = pd.DataFrame(results, columns=cols)
out_df.to_pickle(out_pkl)


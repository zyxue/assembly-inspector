import sys
import re

import pysam

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')

'''This script should be used after the SAM file has already been filtered by

samtools view -h in.bam ref1 ref2 > out.sam
'''

input_sam, output_sam = sys.argv[1:3]
samfile = pysam.AlignmentFile(input_sam)

# read pairs maybe aligned to different references
logging.info('collecting reference names...')
refs = set()
for k, rec in enumerate(samfile):
    refs.add(rec.reference_name)
    refs.add(rec.next_reference_name)
samfile.close()
logging.info('collecting reference names done')

logging.info('filter sam file headers ...')
re_pattern = re.compile(r'\sSN:({0})\s'.format('|'.join(list(refs))))
with open(input_sam, 'rt') as inf:
    with open(output_sam, 'wt') as opf:
        for k, line in enumerate(inf):
            if (k + 1) % 5000 == 0:
                logging.info('processed {0} lines'.format(k + 1))
            if (line.startswith('@SQ') and not re_pattern.search(line)):
                continue
            opf.write(line)
logging.info('filter sam file headers done')

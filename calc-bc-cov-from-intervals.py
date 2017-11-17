import sys
import os
import csv
import pickle
import numpy as np

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')


dd_pkl = './spruce/links10000.scaffolds_renamed.len_dd.pkl'
logging.info('loading {0}'.format(dd_pkl))
with open(dd_pkl, 'rb') as inf:
    ref_len_dd = pickle.load(inf)
logging.info('loading finished'.format(dd_pkl))
available_contigs = set(ref_len_dd.keys())

# interval_file = '/projects/btl/zxue/assembly_correction/spruce/BX/from-pyspark/agg_cov.csv'
interval_file = sys.argv[1]
out_csv = os.path.join(os.path.dirname(interval_file), 'cov-arr.csv')


def write(writer, rn, arr):
    writer.writerow([rn, ' '.join(arr.astype(str).tolist())])

# missing_contigs = set()

with open(out_csv, 'wt') as opf:
    csvwriter = csv.writer(opf)
    with open(interval_file, 'rt') as inf:
        current_rn = None
        num_parsed_contigs = 0
        for k, line in enumerate(inf):
            # if 'beg' in line:   # possible header
            #     continue

            rn, bc, beg, end, _ = line.split(',')
            if rn not in available_contigs:
                continue

            beg = int(beg)
            end = int(end)

            if current_rn is None:
                current_rn = rn
                arr = np.zeros((int(ref_len_dd[rn]),), dtype=int)
            else:
                if current_rn != rn:
                    write(csvwriter, current_rn, arr)
                    num_parsed_contigs += 1

                    current_rn = rn
                    arr = np.zeros((int(ref_len_dd[rn]),), dtype=int)

                    if num_parsed_contigs % 10000 == 0:
                        logging.info('processed {0} contigs'.format(num_parsed_contigs))

                    break

            arr[beg: end] += 1

            if (k + 1) % 1000000 == 0:
                logging.info('processed {0} rows from {1}'.format(k + 1, interval_file))

        write(csvwriter, current_rn, arr)
        num_parsed_contigs += 1

logging.info('parsed {0} contigs in total'.format(num_parsed_contigs))
logging.info('processed {0} rows from {1} in total'.format(k + 1, interval_file))

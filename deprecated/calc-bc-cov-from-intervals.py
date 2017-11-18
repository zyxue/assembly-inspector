import sys
import os
import csv
import pickle
import numpy as np

import h5py

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')


def load_contig_len_dd(pkl):
    logging.info('loading {0}'.format(dd_pkl))
    with open(dd_pkl, 'rb') as inf:
        contig_len_dd = pickle.load(inf)
    logging.info('loading finished'.format(dd_pkl))
    return contig_len_dd



# e.g. interval_file = '/projects/btl/zxue/assembly_correction/spruce/BX/from-pyspark/agg_cov.csv'
interval_file = sys.argv[1]


# e.g. './spruce/links10000.scaffolds_renamed.len_dd.pkl'
contig_len_dd_pkl = sys.argv[2]


# out_csv = os.path.join(os.path.dirname(interval_file), 'cov-arr.csv')
out_h5 = os.path.join(os.path.dirname(interval_file), 'cov-arr.h5')


# def write(writer, rn, arr):
#     writer.writerow([rn, ' '.join(arr.astype(str).tolist())])

def write(h5_writer, rn, arr):
    # bc means barcode coverage
    h5_writer.create_dataset("{0}/bc".format(rn), data=arr)


# missing_contigs = set()

# with open(out_csv, 'wt') as opf:
#     csvwriter = csv.writer(opf)

h5_writer = h5py.File(out_h5, "w")

with open(interval_file, 'rt') as inf:
    current_rn = None
    num_parsed_contigs = 0
    for k, line in enumerate(inf):
        rn, bc, beg, end, _ = line.split(',')
        beg = int(beg)
        end = int(end)

        if current_rn is None:
            current_rn = rn
            arr = np.zeros((int(ref_len_dd[rn]),), dtype=np.uint)
        else:
            if current_rn != rn:
                write(h5_writer, current_rn, arr)
                num_parsed_contigs += 1

                try:
                    arr = np.zeros((int(ref_len_dd[rn]),), dtype=np.uint)
                except KeyError as err:
                    logging.exception(err)
                    continue
                current_rn = rn

                if num_parsed_contigs % 10000 == 0:
                    logging.info('processed {0} contigs'.format(num_parsed_contigs))

        arr[beg: end] += 1

        if (k + 1) % 1000000 == 0:
            logging.info('processed {0} rows from {1}'.format(k + 1, interval_file))

    write(h5_writer, current_rn, arr)
    num_parsed_contigs += 1

h5_writer.close()

logging.info('parsed {0} contigs in total'.format(num_parsed_contigs))
logging.info('processed {0} rows from {1} in total'.format(k + 1, interval_file))

"""calculate read coverage from bedtools genomecov output"""


import sys
import os
import csv
import pickle
import numpy as np

import h5py

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')


def load_pkl(pkl):
    logging.info('loading {0}'.format(pkl))
    with open(pkl, 'rb') as inf:
        contig_len_dd = pickle.load(inf)
    logging.info('loading finished'.format(pkl))
    return contig_len_dd


def write(h5_writer, rn, arr):
    # bc means barcode coverage
    h5_writer.create_dataset("{0}/rc".format(rn), data=arr)


def parse_row(line):
    rn, beg, end, read_cov = line.split('\t')
    beg = int(beg)
    end = int(end)
    read_cov = int(read_cov)
    return rn, beg, end, read_cov


def init_arr(key, len_dd):
    arr = None
    try:
        arr = np.zeros((int(len_dd[key]),), dtype=np.uint)
    except KeyError as err:
        logging.exception(err)
    return arr


def update_arr(arr, beg, end, read_cov):
    if arr is not None:     # due to unfound contig name
        arr[beg: end] += read_cov


def log_num_contig(num_parsed_contigs, interval=10000):
    if num_parsed_contigs % interval == 0:
        logging.info('processed {0} contigs'.format(num_parsed_contigs))


def log_num_rows(num_rows, in_csv, interval=1000000):
    if num_rows % interval == 0:
        logging.info('processed {0} rows from {1}'.format(num_rows, in_csv))


def process(in_csv, contig_len_dd):
    # init variables
    num_parsed_contigs = 0
    arr = None
    curr_rn = None

    with open(in_csv, 'rt') as inf:
        for k, line in enumerate(inf):
            rn, beg, end, read_cov = parse_row(line)

            if curr_rn is None:
                curr_rn = rn
                arr = init_arr(curr_rn, contig_len_dd)
            else:
                if curr_rn != rn:
                    # cleanup previous record
                    if arr is not None:
                        yield (curr_rn, arr)
                        num_parsed_contigs += 1

                    curr_rn = rn
                    arr = init_arr(curr_rn, contig_len_dd)

                    log_num_contig(num_parsed_contigs)

                    # break
            update_arr(arr, beg, end, read_cov)

            log_num_rows(k + 1, in_csv, interval=1000000)

        if arr is not None:
            yield (curr_rn, arr)
            num_parsed_contigs += 1
        logging.info('parsed {0} contigs in total'.format(num_parsed_contigs))
        logging.info('processed {0} rows from {1} in total'.format(k + 1, in_csv))


if __name__ == "__main__":
    # e.g. interval_file = '/projects/btl/zxue/assembly_correction/spruce/BX/from-pyspark/agg_cov.csv'
    in_csv = sys.argv[1]
    out_h5 = os.path.join(os.path.dirname(in_csv), 'read-cov-arr.h5')

    # e.g. './spruce/links10000.scaffolds_renamed.len_dd.pkl'
    contig_len_dd_pkl = sys.argv[2]
    contig_len_dd = load_pkl(contig_len_dd_pkl)

    h5_writer = h5py.File(out_h5, "w")
    for (curr_rn, arr) in process(in_csv, contig_len_dd):
        # print(curr_rn, arr)
        write(h5_writer, curr_rn, arr)

    h5_writer.close()

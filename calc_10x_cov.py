import os
import sys
import pickle

import pysam

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')


"""SAM is 1-based while BAM is 0-based, pysam always follow python convention,
so do my codes"""


def extract_barcode(query_name):
    return query_name.split('_')[-1]


def update_info(old, new):
    """be functional, yey!"""
    return (
        min(old[0], new[0]),
        max(old[1], new[1]),
        old[2] + new[2]         # update read_count
    )


def gen_info_tuple(rec):
    return (
        rec.reference_start,
        rec.reference_end,
        1
    )


def pass_qc(sam_record):
    rec = sam_record
    return not (
        rec.reference_id == -1
        or rec.mapping_quality == 0
        or rec.reference_end is None  # this could be a result of MAPQ=0
    )


def gen_cov_table(input_bam):
    logging.info('reading {0}'.format(input_bam))
    infile = pysam.AlignmentFile(input_bam)

    results = {}
    logging.info('looping {0}'.format(input_bam))
    for k, rec in enumerate(infile):
        if (k + 1) % 1000000 == 0:
            logging.info('processed {0} records'.format(k + 1))

        if not pass_qc(rec):
            continue

        ref_name = rec.reference_name
        bc = extract_barcode(rec.query_name)

        info = gen_info_tuple(rec)

        ref_dd = results.get(ref_name)
        if ref_dd is None:
            results[ref_name] = {bc: info}
        else:
            old_tuple = ref_dd.get(bc)
            if old_tuple is None:
                ref_dd[bc] = info
            else:
                ref_dd[bc] = update_info(ref_dd[bc], info)
    logging.info('looping finished'.format(input_bam))
    return results


def write_results(results, output):
    with open(output, 'wb') as opf:
        pickle.dump(results, opf)


if __name__ == "__main__":
    infile_name = sys.argv[1]
    out_pkl = os.path.join(
        os.path.dirname(infile_name),
        os.path.basename(infile_name).rstrip('.bam') + '.cov-table.pkl'
    )

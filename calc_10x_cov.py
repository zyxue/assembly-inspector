import os
import sys
import pickle

import pandas as pd

import pysam

import logging
logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(message)s')


"""SAM is 1-based while BAM is 0-based, pysam always follow python convention,
so do my codes"""


def extract_barcode(query_name, lib_id=None):
    bc = query_name.split('_')[-1]
    if lib_id is not None:
        bc += '-{0}'.format(lib_id)
    return bc


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


def get_lib_id(input_bam, dd):
    for k in dd.keys():
        if k in input_bam:
            return dd[k]


def gen_cov_table(input_bams, lib_id_dd=None):
    """lib_id: library id, used to disambiguate barcodes from different
    libraries"""
    results = {}
    total_counts = 0
    for ibam in input_bams:
        logging.info('reading {0}'.format(ibam))
        infile = pysam.AlignmentFile(ibam)
        lib_id = get_lib_id(ibam, lib_id_dd)
        logging.info('found lib_id: {0}'.format(lib_id))

        for k, rec in enumerate(infile):
            if not pass_qc(rec):
                continue

            ref_name = rec.reference_name
            bc = extract_barcode(rec.query_name, lib_id)

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
            total_counts += 1
            if (k + 1) % 1000000 == 0:
                logging.info(
                    'processed {0} records from {1}, {2} in total'.format(
                        k + 1, ibam, total_counts))
        logging.info('finished parsing {0}'.format(ibam))

        infile.close()
    logging.info(
        'processed {0} records in total from {1} bam files'.format(
            total_counts, len(input_bams)))
    return results


# def calc_cov(cov_table):
#     """cov_table is a dictionary of dictionaries"""
#     for ref_name in cov_table.keys():
#         for bc in cov_table[ref_name].values():

def write_results(results, output):
    logging.info('dumping results to {0}'.format(output))
    with open(output, 'wb') as opf:
        pickle.dump(results, opf)


if __name__ == "__main__":
    in_bams = list(sorted(sys.argv[1:]))
    out_dir = './spruce'
    out_pkl = os.path.join(
        out_dir,
        'cov-table.pkl'
    )

    lib_id_df = df = pd.read_csv('/projects/spruceup_scratch/psitchensis/Q903/assembly/scaffolding/ARCS/post-LINKS_3-ABySS-Assemblies/alignments/libraries.tsv', sep='\t')
    lib_id_dd = dict(df.values)

    # for i in in_bams:
    #     print(i, get_lib_id(i, lib_id_dd))
    res = gen_cov_table(in_bams, lib_id_dd)
    write_results(res, out_pkl)

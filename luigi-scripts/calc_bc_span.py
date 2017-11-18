#!/usr/bin/env python

import sys
import csv

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


def update_span(old, new):
    """be functional, yey!"""
    return (
        min(old[0], new[0]),
        max(old[1], new[1]),
        old[2] + new[2]         # update read_count
    )


def gen_span_tuple(rec):
    return (
        rec.reference_start,
        rec.reference_end,
        1
    )


# def pass_qc(sam_record):
#     """pass quality check or not"""
#     rec = sam_record
#     return not (
#         rec.reference_id == -1
#         or rec.mapping_quality == 0
#         or rec.reference_end is None  # this could be a result of MAPQ=0
#     )


# def get_lib_id(input_bam, dd):
#     for k in dd.keys():
#         if k in input_bam:
#             return dd[k]


def gen_span_dd_and_read_cov(input_bams, lib_id=''):
    """lib_id: library id, used to disambiguate barcodes from different
    libraries"""
    span_dd = {}

    total_counts = 0
    for ibam in input_bams:
        logging.info('reading {0}'.format(ibam))
        infile = pysam.AlignmentFile(ibam)

        for k, rec in enumerate(infile):
            # if not pass_qc(rec):
            #     continue

            ref_name = rec.reference_name
            bc = extract_barcode(rec.query_name, lib_id)

            span = gen_span_tuple(rec)

            ref_dd = span_dd.get(ref_name)
            if ref_dd is None:
                span_dd[ref_name] = {bc: span}
            else:
                old_tuple = ref_dd.get(bc)
                if old_tuple is None:
                    ref_dd[bc] = span
                else:
                    ref_dd[bc] = update_span(ref_dd[bc], span)
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
    return span_dd


def write_span_results(span_dd, output):
    logging.info('writing results to {0}'.format(output))

    with open(output, 'wt') as opf:
        csvwriter = csv.writer(opf, delimiter='\t')
        total_contigs = len(span_dd.keys())
        for ck, (contig, val_dd) in enumerate(span_dd.items()):
            for (bc, values) in val_dd.items():
                if lib_id:
                    bc += '-{0}'.format(lib_id)
                csvwriter.writerow([contig, bc] + list(values))
            if (ck + 1) % 100000 == 0:
                logging.info('processed {0}/{1} ({2:.2%})'.format(
                    ck + 1, total_contigs, (ck + 1) / total_contigs))
        logging.info('processed {0}/{1} ({2:.2%})'.format(
            ck + 1, total_contigs, (ck + 1) / total_contigs))


if __name__ == "__main__":
    out_csv = sys.argv[1]
    lib_id = sys.argv[2]
    in_bams = list(sorted(sys.argv[3:]))  # e.g. from the same library

    res = gen_span_dd_and_read_cov(in_bams, lib_id)
    write_span_results(res, out_csv)

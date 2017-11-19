#!/usr/bin/env python

import sys
import csv

import numpy as np
import h5py
import pysam

import logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s|%(levelname)s|%(message)s')


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


# def gen_span_tuple(rec):
#     return (
#         rec.reference_start,
#         rec.reference_end,
#         1
#     )


def parse_bam_record(rec):
    rn = rec.reference_name
    beg = int(rec.reference_start)
    end = int(rec.reference_end)
    return rn, beg, end


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


# def update_read_cov_arr(arr, rec):
#     arr[rec.reference_start: rec.reference_end] += 1


def gen_barcode_and_read_cov(input_bam, lib_id, contig_len_dd, out_h5):
    """lib_id: library id, used to disambiguate barcodes from different
    libraries"""
    rc_arr = None               # read coverage
    bc_arr = None               # barcode covervage
    curr_rn = None              # current ref name
    bc_span_dd = None
    num_parsed_contigs = 0

    logging.info('reading {0}'.format(input_bam))
    infile = pysam.AlignmentFile(input_bam)

    h5_writer = h5py.File(out_h5, "w")

    for k, rec in enumerate(infile):
        # if not pass_qc(rec):
        #     continue
        rn, beg, end = parse_bam_record(rec)

        # process barcode span
        bc = extract_barcode(rec.query_name, lib_id)
        span = (beg, end, 1)    # 1 is read count

        if curr_rn is None:
            curr_rn = rn
            contig_len = contig_len_dd[curr_rn]
            rc_arr = np.zeros((contig_len,), dtype=np.uint)
            bc_arr = np.zeros((contig_len,), dtype=np.uint)
            bc_span_dd = {bc: span}
        else:
            if curr_rn != rn:
                # write rc_arr to h5
                h5_writer.create_dataset("{0}/rc".format(curr_rn), data=rc_arr)

                # fillup bc_arr from info in bc_span_dd
                for (bc, (span_beg, span_end, _)) in bc_span_dd.items():
                    bc_arr[span_beg:span_end] += 1
                h5_writer.create_dataset("{0}/bc".format(curr_rn), data=bc_arr)

                num_parsed_contigs += 1

                # init variables
                curr_rn = rn
                contig_len = contig_len_dd[curr_rn]
                rc_arr = np.zeros((contig_len,), dtype=np.uint)
                bc_arr = np.zeros((contig_len,), dtype=np.uint)
                bc_span_dd = {bc: span}

        # update read cov
        rc_arr[beg:end] += 1

        # update span_dd
        if bc in bc_span_dd:
            bc_span_dd[bc] = update_span(bc_span_dd[bc], span)
        else:
            bc_span_dd[bc] = span

        if (k + 1) % 1000000 == 0:
            logging.info(
                'processed {0} records from {1}'.format(k + 1, input_bam))

    # write rc_arr to h5
    h5_writer.create_dataset("{0}/rc".format(curr_rn), data=rc_arr)

    # fillup bc_arr from info in bc_span_dd
    for (bc, (span_beg, span_end, _)) in bc_span_dd.items():
        bc_arr[span_beg:span_end] += 1
    h5_writer.create_dataset("{0}/bc".format(curr_rn), data=bc_arr)

    num_parsed_contigs += 1

    logging.info('Parsed {0} contigs intotal'.format(num_parsed_contigs))
    logging.info('finished parsing {0}'.format(input_bam))

    h5_writer.close()

# def write_span_results(span_dd, output):
#     logging.info('writing results to {0}'.format(output))

#     with open(output, 'wt') as opf:
#         csvwriter = csv.writer(opf, delimiter='\t')
#         total_contigs = len(span_dd.keys())
#         for ck, (contig, val_dd) in enumerate(span_dd.items()):
#             for (bc, values) in val_dd.items():
#                 if lib_id:
#                     bc += '-{0}'.format(lib_id)
#                 csvwriter.writerow([contig, bc] + list(values))
#             if (ck + 1) % 100000 == 0:
#                 logging.info('processed {0}/{1} ({2:.2%})'.format(
#                     ck + 1, total_contigs, (ck + 1) / total_contigs))
#         logging.info('processed {0}/{1} ({2:.2%})'.format(
#             ck + 1, total_contigs, (ck + 1) / total_contigs))


if __name__ == "__main__":
    out_csv = sys.argv[1]
    lib_id = sys.argv[2]
    in_bam = sys.argv[3]  # e.g. already merged from the same library

    # gen_barcode_and_read_cov(in_bam, lib_id)
    # write_span_results(res, out_csv)

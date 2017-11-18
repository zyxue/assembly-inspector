import os
import json
import subprocess
import itertools

import numpy as np
import pandas as pd
import h5py
import luigi


def zprint(s, *a, **ka):
    print('*' * 10 + str(s), *a, **ka)


class FilterBam(luigi.Task):
    bam = luigi.Parameter()
    contigs = luigi.ListParameter()

    def requires(self):
        return []

    def output(self):
        # out_path = os.path.join(self.out_dir, out_fn)

        out_fn = '{0}.{1}-contigs.bam'.format(
            os.path.basename(self.bam).rstrip('.bam'), len(self.contigs))
        out_path = os.path.join(os.path.dirname(self.bam), out_fn)
        return luigi.LocalTarget(out_path)

    def run(self):
        contigs = ' '.join([str(_) for _ in self.contigs])
        cmd = 'samtools view -hb {in_bam} {contigs} > {out_bam}'.format(
            in_bam=self.bam,
            contigs=contigs,
            out_bam=self.output().fn)
        zprint(cmd)
        subprocess.call(cmd, shell=True, executable="/bin/bash")


class FilterAndMergeBamsFromSameLibrary(luigi.Task):
    lib_id = luigi.Parameter()
    bams = luigi.ListParameter()
    contigs = luigi.ListParameter()
    out_dir = luigi.Parameter()

    def requires(self):
        for bam in self.bams:
            yield FilterBam(bam=bam, contigs=self.contigs)

    def output(self):
        return luigi.LocalTarget(os.path.join(
            self.out_dir, '{0}.filtered.merged.bam'.format(self.lib_id)))

    def run(self):
        cmd = 'samtools merge {out_bam} {in_bams}'.format(
            out_bam=self.output().fn,
            in_bams=' '.join([_.fn for _ in self.input()]),
        )
        zprint(cmd)
        subprocess.call(cmd, shell=True, executable="/bin/bash")


def gen_contig_length_dict_from_fai(fai, contigs):
    res = {}
    contigs = set(contigs)
    count = 0
    target = len(contigs)
    with open(fai, 'rt') as inf:
        for line in inf:
            ref_name, contig_len = line.split('\t')[:2]
            if ref_name in contigs:
                res[ref_name] = int(contig_len)
                count += 1
                if count == target:  # no need to loop through the whole file
                    break
    return res


class CalculateCoveragesForBamsFromSameLibrary(luigi.Task):
    """
    Calculate both barcode and read coverages and store them in hdf5
    """
    lib_id = luigi.Parameter()
    bams = luigi.ListParameter()  # {lib_id: [1.bam, 2.bam]}
    contigs = luigi.ListParameter()
    out_dir = luigi.Parameter()
    fai = luigi.Parameter()     # fa index, used for calculate contig length

    def requires(self):
        return FilterAndMergeBamsFromSameLibrary(
            lib_id=self.lib_id,
            bams=self.bams,
            contigs=self.contigs,
            out_dir=self.out_dir
        )

    def output(self):
        # bc_span_csv = os.path.join(self.out_dir, '{0}.bc_span.csv'.format(self.lib_id))
        cov_h5 = os.path.join(self.out_dir, '{0}.h5'.format(self.lib_id))
        return luigi.LocalTarget(cov_h5)

    def run(self):
        from calc_cov import gen_barcode_and_read_cov

        contig_len_dd = gen_contig_length_dict_from_fai(self.fai, self.contigs)
        gen_barcode_and_read_cov(
            self.input().fn,
            self.lib_id,
            contig_len_dd,
            self.output().fn
        )


class CalcAndSumCoveragesForGroupedBamsFromMultiLibraries(luigi.Task):
    """
    Calculate both barcode and read coverages
    """
    grouped_bams = luigi.DictParameter()  # {lib_id: [1.bam, 2.bam]}
    contigs = luigi.ListParameter()
    out_dir = luigi.Parameter()
    fai = luigi.Parameter()     # fa index, used for calculate contig length

    def requires(self):
        for lib_id in self.grouped_bams:
            bams = self.grouped_bams[lib_id]
            yield CalculateCoveragesForBamsFromSameLibrary(
                lib_id=lib_id,
                bams=bams,
                contigs=self.contigs,
                out_dir=self.out_dir,
                fai=self.fai
            )

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_dir, 'total.h5'))

    def run(self):
        ind_h5_dd = {}          # ind: individual
        for i in self.input():
            ind_h5_dd[i.fn] = h5py.File(i.fn, 'r')

        with h5py.File(self.output().fn, 'w') as opf:
            for rn in self.contigs:
                total_rc, total_bc = None, None
                rc_key = '{0}/rc'.format(rn)
                bc_key = '{0}/bc'.format(rn)
                for i in self.input():
                    # update read coverage
                    if total_rc is None:
                        total_rc = ind_h5_dd[i.fn][rc_key].value
                    else:
                        total_rc += ind_h5_dd[i.fn][rc_key].value

                    # update barcode coverage
                    if total_bc is None:
                        total_bc = ind_h5_dd[i.fn][bc_key].value
                    else:
                        total_bc += ind_h5_dd[i.fn][bc_key].value

                opf.create_dataset(rc_key, data=total_rc)
                opf.create_dataset(bc_key, data=total_bc)

        for i in self.input():
            ind_h5_dd[i.fn].close()

if __name__ == '__main__':
    luigi.run()

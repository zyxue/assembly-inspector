import os
import json
import subprocess
import itertools

import numpy as np
import pandas as pd
import luigi


# class SelectLongestContigs(luigi.Task):
#     in_fai = luigi.Parameter()
#     top_n = luigi.IntParameter(default=3)
#     out_dir = luigi.Parameter()

#     def requires(self):
#         return []

#     def output(self):
#         return luigi.LocalTarget(
#             os.path.join(self.out_dir,
#                          'top_{0}_longest_contigs.csv'.format(self.top_n)))

#     def run(self):
#         # for format of fai: http://www.htslib.org/doc/faidx.html
#         df_len = pd.read_csv(
#             self.in_fai, sep='\t', header=None,
#             usecols=[0, 1], dtype={0: str, 1: np.uint})
#         df_len.columns = ['ref_name', 'len']
#         df_len.sort_values('len', ascending=False, inplace=True)
#         df_len.head(self.top_n).to_csv(self.output().fn, index=False)

def zprint(s, *a, **ka):
    print('*' * 10 + s, *a, **ka)


class FilterBam(luigi.Task):
    bam = luigi.Parameter()
    contigs = luigi.ListParameter()

    def requires(self):
        return []

    def output(self):
        # out_path = os.path.join(self.out_dir, out_fn)

        out_fn = '{0}.filtered.bam'.format(
            os.path.basename(self.bam).rstrip('.bam'))
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


class CalculateCoverages(luigi.Task):
    """
    Calculate both barcode and read coverages
    """
    grouped_bams = luigi.DictParameter()  # {lib_id: [1.bam, 2.bam]}
    contigs = luigi.ListParameter()
    fai = luigi.Parameter()     # fa index, used for calculate contig length
    out_dir = luigi.Parameter()

    def requires(self):
        for lib_id in self.grouped_bams:
            bams = self.grouped_bams[lib_id]
            yield FilterAndMergeBamsFromSameLibrary(
                lib_id=lib_id,
                bams=bams,
                contigs=self.contigs,
                out_dir=self.out_dir)

    def run(self):
        print([_.fn for _ in self.input()])
        # calculate bc coverage
        # calculate read coverage
        pass



if __name__ == '__main__':
    luigi.run()

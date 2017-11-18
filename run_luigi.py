import os
import json
import subprocess

import numpy as np
import pandas as pd
import luigi


class SelectLongestContigs(luigi.Task):
    in_fai = luigi.Parameter()
    top_n = luigi.IntParameter(default=3)
    out_dir = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.out_dir,
                         'top_{0}_longest_contigs.csv'.format(self.top_n)))

    def run(self):
        # for format of fai: http://www.htslib.org/doc/faidx.html
        df_len = pd.read_csv(
            self.in_fai, sep='\t', header=None,
            usecols=[0, 1], dtype={0: str, 1: np.uint})
        df_len.columns = ['ref_name', 'len']
        df_len.sort_values('len', ascending=False, inplace=True)
        df_len.head(self.top_n).to_csv(self.output().fn, index=False)


class FilterBam(luigi.Task):
    bam = luigi.Parameter()
    in_fai = luigi.Parameter()
    top_n = luigi.IntParameter(default=3)
    out_dir = luigi.Parameter()

    def requires(self):
        return SelectLongestContigs(
            in_fai=self.in_fai,
            top_n=self.top_n,
            out_dir=self.out_dir,
        )

    def output(self):
        out_fn = '{0}.filtered.bam'.format(
            os.path.basename(self.bam).rstrip('.bam'))
        # out_path = os.path.join(self.out_dir, out_fn)
        out_path = os.path.join(os.path.dirname(self.bam), out_fn)
        print(out_path)
        return luigi.LocalTarget(out_path)

    def run(self):
        contigs = ' '.join(
            pd.read_csv(self.input().fn).ref_name.astype(str).values.tolist())

        cmd = 'samtools view -hb {in_bam} {contigs} > {out_bam}'.format(
            in_bam=self.bam,
            contigs=contigs,
            out_bam=self.output().fn)
        print(cmd)
        subprocess.call(cmd, shell=True, executable="/bin/bash")


class FilterBams(luigi.WrapperTask):
    bams = luigi.ListParameter()
    in_fai = luigi.Parameter()
    top_n = luigi.IntParameter(default=3)
    out_dir = luigi.Parameter()

    def requires(self):
        for bam in self.bams:
            yield FilterBam(
                bam=bam, in_fai=self.in_fai,
                top_n=self.top_n, out_dir=self.out_dir)

                 
if __name__ == '__main__':
    luigi.run()

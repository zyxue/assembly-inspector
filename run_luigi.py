import os
import subprocess

import numpy as np
import pandas as pd
import luigi


class SelectLongestContigs(luigi.Task):
    top_n = luigi.IntParameter(default=3)
    out_dir = luigi.Parameter()
    in_fai = luigi.Parameter()

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

if __name__ == '__main__':
    luigi.run()

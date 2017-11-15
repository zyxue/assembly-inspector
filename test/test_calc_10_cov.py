import sys
sys.path.insert(0, '..')

from calc_10x_cov import gen_cov_table

test_bam = './E00455-A64450_links10000scaffolds_renamed-sorted.test.bam'
# expected results
test_csv = './E00455-A64450_links10000scaffolds_renamed-sorted.test.csv'
results = gen_cov_table(test_bam)

# tuple means beg, end, read_count
assert results['1']['AAACCCATCCGGCCAA'] == (299267, 304662, 15)
assert results['1']['ACAAGGGGTGAAATCA'] == (236633, 244968, 44)
assert results['2']['TTTGCGCAGCGAGAAA'] == (249958, 250084, 1)
assert results['2']['GATTACGTCGACGTAT'] == (220652, 304213, 127)

with open(test_csv, 'rt') as inf:
    for k, line in enumerate(inf):
        if k == 0:              # skip header
            continue
        (ref_name, bc, beg, end, c) = line.strip().split(',')
        beg, end, c = int(beg), int(end), int(c)
        assert results[ref_name][bc] == (beg, end, c)

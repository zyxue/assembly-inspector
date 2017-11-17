import sys
sys.path.insert(0, '..')

from calc_read_cov import process


def test1():
    mock_cov_file = './mock_genomecov.tsv'
    mock_contig_len_dd = {
        '1': 10,
        '2': 20
    }

    # {reference_name: np.array}
    results = {
        '1': [3, 3, 3, 3, 3, 10, 10, 10, 10, 10],
        '2': [2, 2, 2, 1, 1, 1, 1, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    print(mock_contig_len_dd)
    for (curr_rn, arr) in process(mock_cov_file, mock_contig_len_dd):
        print(curr_rn, arr)
        assert results[curr_rn] == arr.tolist()

# Setup

```
virtualenv venv
source venv/bin
pip install -r requirements.txt

# start luigi scheduler, listening at 8082 by default
luigid
```

# Example commands

```
python run_luigi.py \
           CalcAndSumCoveragesForGroupedBamsFromMultiLibraries --workers=11 \
           --out-dir top-100 \
           --grouped-bams '{"A64450": ["spruce/BX/A64450/E00455/BX.sort-by-index.bam", "spruce/BX/A64450/E00457/BX.sort-by-index.bam"], "A64451": ["spruce/BX/A64451/E00455/BX.sort-by-index.bam", "spruce/BX/A64451/E00457/BX.sort-by-index.bam"], "A64452": ["spruce/BX/A64452/E00455/BX.sort-by-index.bam", "spruce/BX/A64452/E00457/BX.sort-by-index.bam"], "A64453": ["spruce/BX/A64453/E00455/BX.sort-by-index.bam", "spruce/BX/A64453/E00457/BX.sort-by-index.bam"], "E00132": ["spruce/BX/E00132/E00458/BX.sort-by-index.bam", "spruce/BX/E00132/E00459/BX.sort-by-index.bam", "spruce/BX/E00132/SIGAE3/BX.sort-by-index.bam"]}' \
           --contigs '["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "209", "14", "15", "16", "17", "18", "128", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "2634", "32", "33", "34", "36", "35", "37", "38", "2017", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "166", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "92", "76", "142", "124", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "93"]' \
           --fai spruce/links10000.scaffolds_renamed.fa.fai
```

start from a intermediate task

```
python run_luigi.py \
           CalcAndSumCoveragesForGroupedBamsFromMultiLibraries --workers=11 \
           --out-dir top-50 \
           --grouped-bams '{"A64450": ["spruce/BX/A64450/E00455/BX.sort-by-index.bam", "spruce/BX/A64450/E00457/BX.sort-by-index.bam"], "A64451": ["spruce/BX/A64451/E00455/BX.sort-by-index.bam", "spruce/BX/A64451/E00457/BX.sort-by-index.bam"], "A64452": ["spruce/BX/A64452/E00455/BX.sort-by-index.bam", "spruce/BX/A64452/E00457/BX.sort-by-index.bam"], "A64453": ["spruce/BX/A64453/E00455/BX.sort-by-index.bam", "spruce/BX/A64453/E00457/BX.sort-by-index.bam"], "E00132": ["spruce/BX/E00132/E00458/BX.sort-by-index.bam", "spruce/BX/E00132/E00459/BX.sort-by-index.bam", "spruce/BX/E00132/SIGAE3/BX.sort-by-index.bam"]}' \
           --contigs '["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "209", "14", "15", "16", "17", "18", "128", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "2634", "32", "33", "34", "36", "35", "37", "38", "2017", "39", "40", "41", "42", "43", "44", "45", "46"]' \
           --fai spruce/links10000.scaffolds_renamed.fa.fai
````

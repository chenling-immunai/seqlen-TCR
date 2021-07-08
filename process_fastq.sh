gsutil cp gs://augustus-automatically-managed/sequencing_fastq/30-461364038_fastq_v1/Feature*2020-12-10-1-3* .
for f in *.fastq.gz
time seqtk trimfq -e 50 GEX-500_3-2020-12-18-2-1_S1_L004_R1_001.fastq.gz > GEX-500_3-2020-12-18-2-1_S1_L004_R1_001.100bp.fastq  

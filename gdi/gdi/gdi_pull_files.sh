#!/bin/sh

source .env

rm -rf data/*.csv data/*.txt

declare -a FILES=("gdi_files.csv" "gdi_ped.csv" "gdi_phenopacket.csv" "gdi_checksums.csv")

for FILE in "${FILES[@]}"
do
  echo "Retrieving ${OUTPUT_DIR}/${FILE}"
  rsync -av "airlock+gearshift:${OUTPUT_DIR}/${FILE}" "data/${FILE}"
done

rsync -av airlock+gearshift:${ENTRY_DIR}/md5sums_current_files.txt data/
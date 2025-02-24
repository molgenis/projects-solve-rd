#!/bin/sh

source .env

declare -a FILES=("gdi/gdi_build_files.py")

for FILE in "${FILES[@]}"
do
    echo "Sending ${FILE} to ${TARGET_DIR}"
    rsync -av "${FILE}" airlock+gearshift:${TARGET_DIR}
done

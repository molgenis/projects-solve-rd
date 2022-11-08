
for file in *.csv
do
  newTableEndpoint="${file/rd3_/solverd_}"
  importAs="${newTableEndpoint/.csv/}"
  echo "Importing data into $importAs"
  mcmd import -p "$file" --as "$importAs"
done
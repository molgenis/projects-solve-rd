function showHelp() {
	#
	# Display commandline help on STDOUT.
	#
	cat <<EOH
===============================================================================================================
Script to create patch directory from master directory.
Usage:
	$(basename $0) OPTIONS
Options:
	-h   Show this help.
	-m   master directory (default=/groups/umcg-solve-rd/tmp01/releases/master/)
	-p   patch directory name. (example freeze1-patch1: default=patch-DATE_TIME)
	-w   workdir (default=/groups/umcg-solve-rd/tmp01/releases/)

===============================================================================================================
EOH
	trap - EXIT
	exit 0
}


while getopts "m:p:w:h" opt;
do
	case $opt in h)showHelp;; m)master="${OPTARG}";; p)patchName="${OPTARG}";; w)workDir="${OPTARG}";; f);;
	esac
done

if [[ -z "${workDir:-}" ]]; then workDir="$( pwd )" ; fi ; echo "workDir=${workDir}"
if [[ -z "${master:-}" ]]; then master="${workDir}/master" ; fi ; echo "master=${master}"
if [[ -z "${patchName:-}" ]]; then patchName=""patch-"$(date '+%Y-%m-%d_%H:%M:%S')" ; fi ; echo "patchName=${patchName}"

masterDir=$(basename ${master})
#
## create patch dir based on latest files.
#
for i in $(ls -1 "${master}/"); do
	base=$(basename ${i})
	echo ${base}
	for j in $(ls -1 ${master}/${i}) ; do echo ${j%%.*} >> ${workDir}/samplesNames.txt ;done
		cat ${workDir}/samplesNames.txt | sort -V -u > ${workDir}/uniekeSamples.txt
		cd ${master}/${i}

		while read line ; do ls -1 ${line}* | sort -V | tail -1 ; done< ${workDir}/uniekeSamples.txt > ${workDir}/latestSamples.txt
		mkdir -p ${workDir}/${patchName}/${i}
		while read line ; do ln -s ../../${masterDir}/${i}/${line} ${workDir}/${patchName}/${i}/${line};done<${workDir}/latestSamples.txt

		rm ${workDir}/samplesNames.txt ${workDir}/uniekeSamples.txt ${workDir}/latestSamples.txt
		cd -
done

#
## diff master with created patch
#
cd "${workDir}"
find "master/" | sed 's,^[^/]*/,,' | sort > master.txt && find "${patchName}/" | sed 's,^[^/]*/,,' | sort > patch.txt

echo -e "${masterDir}\t\t\t\t\t\t\t${patchName}" > "${workDir}/${patchName}/${patchName}.diff"
diff -y master.txt patch.txt >> "${workDir}/${patchName}/${patchName}.diff"
cd ${patchName}
echo -e "${masterDir}\t\t\t\t\t\t${patchName}" > "${patchName}.small"
grep '<' "${patchName}.diff" >> "${patchName}.small"
cd -

#while read line ; do echo ${line%%.*} | sort -u > ${patchName}.uniq; done< ${patchName}.small 

#for i in $(cat ${patchName}.uniq)
#do 
#	base=$(basename $i)
#	replacement=$(fgrep ${base} */*)
#	echo "${base} was replaced by ${replacement}"
#done


rm master.txt patch.txt



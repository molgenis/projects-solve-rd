Script to syncronize directories from Fender with Gearshift and Cher-ami. 

#
##  current crontab on fender
#

#
## rsyncs every night at 5AM and 6AM to cher-ami, detail in etc/datf.cfg,ditf.cfg
1 5 * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; /admin/solve-rd-dm/synchronize/rsync.sh -g solve-rd -l TRACE -r ditf.cfg"
2 6 * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; /admin/solve-rd-dm/synchronize/rsync.sh -g solve-rd -l TRACE -r datf.cfg"

#
## rsyncs every night at 1AM to gearshift, details in etc/sandbox.cfg
0 1 * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; /admin/solve-rd-dm/synchronize/rsync.sh -g solve-rd -l TRACE -r sandbox.cfg"

#
## rsync share dir from sftp every 15 minutes.
3-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d snv-indel -l FATAL -r datf-share.cfg"
4-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d transcriptomics -l FATAL -r datf-share.cfg"
5-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d roh_relatedness -l FATAL -r datf-share.cfg"
6-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d proteomics -l FATAL -r datf-share.cfg"
7-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d meta-analysis -l FATAL -r datf-share.cfg"
8-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d denovo -l FATAL -r datf-share.cfg"
9-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d cnv -l FATAL -r datf-share.cfg"

10-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d euro-nmd -l FATAL -r ditf-share.cfg"
11-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d genturis -l FATAL -r ditf-share.cfg"
12-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d ithaca -l FATAL -r ditf-share.cfg"
13-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d rnd -l FATAL -r ditf-share.cfg"
14-59/15 * * * * /bin/bash -c "export SOURCE_HPC_ENV="True"; ~/synchronize/rsync.sh -g solve-rd -d udn-spain -l FATAL -r ditf-share.cfg"

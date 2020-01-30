#!/bin/bash

dataset="kernel"

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "default dataset <- $dataset"
fi

kernel_path="/home/dataset/kernel_8k/"
vmdk_path="/home/dataset/vmdk_4k/"
rdb_path="/home/dataset/rdb_4k/"
synthetic_path="/home/dataset/synthetic_8k/"

# path: where trace files locate
# fcs: the restore cache size
case $dataset in
    "kernel") 
        path=$kernel_path
        rcs=128
        ;;
    "vmdk")
        path=$vmdk_path
        rcs=1024
        ;;
    "rdb") 
        path=$rdb_path
        rcs=1024
        ;;
    "synthetic") 
        path=$synthetic_path
        rcs=1024
        ;;
    *) 
        echo "Wrong dataset!"
        exit 1
        ;;
esac

# ./rebuild would clear data of previous experiments
# ./destor executes a backup job
#   (results are written to backup.log)
# ./destor -rN executes a restore job under various restore cache size
#   (results are written to restore.log)

# EDPL+HAR
i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index exact physical" -p"rewrite-enable-har yes" -p"rewrite-har-utilization-threshold 0.4" -p"rewrite-har-rewrite-limit 1" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

# NDPL+HAR
i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index near-exact physical" -p"fingerprint-index-sampling-method uniform 128" -p"rewrite-enable-har yes" -p"rewrite-har-utilization-threshold 0.4" -p"rewrite-har-rewrite-limit 1" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

# EDLL+HAR
i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index exact logical" -p"fingerprint-index-sampling-method random 256" -p"rewrite-enable-har yes" -p"rewrite-har-utilization-threshold 0.4" -p"rewrite-har-rewrite-limit 1" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

# NDLL+HAR
i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index near-exact logical" -p"fingerprint-index-sampling-method random 128" -p"fingerprint-index-segment-selection top 4" -p"fingerprint-index-segment-prefetching 4" -p"rewrite-enable-har yes" -p"rewrite-har-utilization-threshold 0.4" -p"rewrite-har-rewrite-limit 1" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

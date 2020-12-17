#!/usr/bin/python3
#
#

from datetime import datetime, timedelta
import time
import os
import re
from prometheus_client import Gauge, start_http_server


buckets_list = []
backup_start_string = ''
backup_end_string = ''
backup_time = timedelta(seconds=0)
backup_start_datetime = datetime.now()
backup_end_datetime = datetime.now()
cur_time = time.time()
local_utc_offset_time_sec = (datetime.fromtimestamp(cur_time) - datetime.utcfromtimestamp(cur_time)).total_seconds()


def get_bytes(size):
    if size == "0":
        return int(0)
    size_bytes = float(size[:-1])
    suffix = size[-1]

    if suffix == 'K':
        return int(size_bytes * float(1024))
    elif suffix == 'M':
        return int(size_bytes * float(1024) * float(1024))
    elif suffix == 'G':
        return int(size_bytes * float(1024) * float(1024) * float(1024))
    elif suffix == 'T':
        return int(size_bytes * float(1024) * float(1024) * float(1024) * float(1024))

    return 0


def get_data_from_log_file():

    global buckets_list
    global backup_start_string
    global backup_end_string
    global backup_time
    global backup_start_datetime
    global backup_end_datetime

    buckets_list = []
    backup_start_string = ''
    backup_end_string = ''
    backup_time = timedelta(seconds=0)

    ## get last backup log from whole log file
    in_log_file = open('/var/log/backup_arh/minio_backup-last.log','r')

    # take last_backup_log_reversed form whole log file
    last_backup_log_reversed = []
    last_log = 0
    for line in reversed(list(in_log_file)):
        if "minio_backup.sh: Job finished" in line.rstrip():
            last_log = 1
        if last_log == 1:
            last_backup_log_reversed.append(line.rstrip())
            if "minio_backup.sh: Job started at" in line.rstrip():
                break

    in_log_file.close()

    # create last_backup_log from last_backup_log_reversed
    last_backup_log = []
    for line in reversed(last_backup_log_reversed):
        last_backup_log.append(line)


    ## get backup start-end times
    for line in last_backup_log:
        if "minio_backup.sh: Job started at" in line:
            backup_start_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            backup_start_datetime = datetime.strptime(backup_start_string, "%Y.%m.%d %H:%M:%S")

        if "minio_backup.sh: Job finished" in line:
            backup_end_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            backup_end_datetime = datetime.strptime(backup_end_string, "%Y.%m.%d %H:%M:%S")

    backup_time = backup_end_datetime - backup_start_datetime

    #print(backup_time.seconds)


    # get information of buckets backup time
    bucket_dict = {}
    buckets_list = []
    for line in last_backup_log:
        if "minio_backup.sh: Start syncing bucket" in line:
            bucket_start_time_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            bucket_start_time_datetime = datetime.strptime(bucket_start_time_string, "%Y.%m.%d %H:%M:%S")
            bucket_name = re.split(r'[\s]', line)[4][1:][:-1]
            bucket_checked = 0
            bucket_checked_total = 0

        if ("Transferred:" in line and "Bytes" in line):
            bucket_transferred_bytes = re.search(r'[0-9]+\.{0,1}[0-9]*[KMGT]{0,1}', line).group(0)

        if "Transferred:" in line:
            bucket_transferred = re.search(r'[0-9]+', line).group(0)
            bucket_transferred_total = re.search(r'[0-9]+', re.split(r'[/]', line)[1]).group(0)

        if "Checks:" in line:
            bucket_checked = re.search(r'[0-9]+', line).group(0)
            bucket_checked_total = re.search(r'[0-9]+', re.split(r'[/]', line)[1]).group(0)

        if "minio_backup.sh: Finished syncing bucket" in line:
            bucket_end_time_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            bucket_end_time_datetime = datetime.strptime(bucket_end_time_string, "%Y.%m.%d %H:%M:%S")

            bucket_time = bucket_end_time_datetime - bucket_start_time_datetime

#	    print(bucket_name)
#	    print(bucket_start_time_datetime)
#	    print(bucket_end_time_datetime)
#	    print(bucket_time)
#	    print(bucket_transferred
#            print(bucket_transferred_total)
#	    if bucket_checked != 0:
#		print(bucket_checked)

            bucket_dict['name'] = bucket_name
            bucket_dict['start_time'] = bucket_start_time_datetime
            bucket_dict['end_time'] = bucket_end_time_datetime
            bucket_dict['backup_time'] = bucket_time.seconds
            bucket_dict['transferred_files'] = bucket_transferred
            bucket_dict['transferred_files_bytes'] = get_bytes(bucket_transferred_bytes)
            bucket_dict['checked_files'] = bucket_checked
            bucket_dict['transferred_total_files'] = bucket_transferred_total
            bucket_dict['checked_total_files'] = bucket_checked_total

            #print(bucket_dict)
            buckets_list.append(bucket_dict.copy())



bucket_backup_start_time_metric = Gauge('bucket_backup_start_time', 'Time of start last backup')
bucket_backup_end_time_metric = Gauge('bucket_backup_end_time', 'Time of end last backup')
#bucket_backup_duration_time_metric = Gauge('bucket_backup_duration_time', 'Duration time of last backup')

bucket_backup_duration_times_metric = Gauge('bucket_backup_duration_times', 'Duration times of particular bucket backup', ['bucket_name'])
bucket_backup_transferred_files_metric = Gauge('bucket_backup_transferred_files', 'Number of file, transferred while bucket backup', ['bucket_name'])
bucket_backup_transferred_files_bytes_metric = Gauge('bucket_backup_transferred_files_bytes', 'Size of files (in bytes), transferred while bucket backup', ['bucket_name'])
bucket_backup_checked_files_metric = Gauge('bucket_backup_checked_files', 'Number of file, checked while bucket backup', ['bucket_name'])
bucket_backup_transferred_total_files_metric = Gauge('bucket_backup_transferred_total_files', 'Total number of files, needed to transfer while bucket backup', ['bucket_name'])
bucket_backup_checked_total_files_metric = Gauge('bucket_backup_checked_total_files', 'Total number of files, needed to check while bucket backup', ['bucket_name'])


start_http_server(8000)
while True:


    get_data_from_log_file()


#    bucket_backup_duration_time_metric.set(backup_time.seconds)
    bucket_backup_start_time_metric.set(int((backup_start_datetime - datetime.utcfromtimestamp(0)).total_seconds()) - local_utc_offset_time_sec)
    bucket_backup_end_time_metric.set(int((backup_end_datetime - datetime.utcfromtimestamp(0)).total_seconds()) - local_utc_offset_time_sec)

    for bucket in buckets_list:
        #print(bucket['backup_time'])
        bucket_backup_duration_times_metric.labels(bucket['name']).set(bucket['backup_time'])
        bucket_backup_transferred_files_metric.labels(bucket['name']).set(bucket['transferred_files'])
        bucket_backup_transferred_files_bytes_metric.labels(bucket['name']).set(bucket['transferred_files_bytes'])
        bucket_backup_checked_files_metric.labels(bucket['name']).set(bucket['checked_files'])
        bucket_backup_transferred_total_files_metric.labels(bucket['name']).set(bucket['transferred_total_files'])
        bucket_backup_checked_total_files_metric.labels(bucket['name']).set(bucket['checked_total_files'])


    time.sleep(120)

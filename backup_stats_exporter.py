#!/usr/bin/python3
#
#

import datetime
import time
import os
import re
from prometheus_client import Gauge, start_http_server


buckets_list = []
backup_start_string = ''
backup_end_string = ''
backup_time = datetime.timedelta(seconds=0)

def get_data_from_log_file():

    global buckets_list
    global backup_start_string
    global backup_end_string
    global backup_time

    buckets_list = []
    backup_start_string = ''
    backup_end_string = ''
    backup_time = datetime.timedelta(seconds=0)

    ## get last backup log from whole log file
    in_log_file = open('/var/log/backup_arh/minio_backup-last.log','r')

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

    last_backup_log = []
    for line in reversed(last_backup_log_reversed):
        last_backup_log.append(line)


    ## get backup start-end times
    for line in last_backup_log:
        if "minio_backup.sh: Job started at" in line:
            backup_start_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            backup_start_datetime = datetime.datetime.strptime(backup_start_string, "%Y.%m.%d %H:%M:%S")

        if "minio_backup.sh: Job finished" in line:
            backup_end_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            backup_end_datetime = datetime.datetime.strptime(backup_end_string, "%Y.%m.%d %H:%M:%S")

    backup_time = backup_end_datetime - backup_start_datetime

    #print(backup_time.seconds)


    # get information of buckets backup time
    bucket_dict = {}
    buckets_list = []
    for line in last_backup_log:
        if "minio_backup.sh: Start syncing bucket" in line:
            bucket_start_time_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            bucket_start_time_datetime = datetime.datetime.strptime(bucket_start_time_string, "%Y.%m.%d %H:%M:%S")
            bucket_name = re.split(r'[\s]', line)[4][1:][:-1]
            bucket_checked = 0
            bucket_checked_total = 0

        if "Transferred:" in line:
            bucket_transferred = re.search(r'[0-9]+', line).group(0)
            bucket_transferred_total = re.search(r'[0-9]+', re.split(r'[/]', line)[1]).group(0)


        if "Checks:" in line:
            bucket_checked = re.search(r'[0-9]+', line).group(0)
            bucket_checked_total = re.search(r'[0-9]+', re.split(r'[/]', line)[1]).group(0)

        if "minio_backup.sh: Finished syncing bucket" in line:
            bucket_end_time_string = re.search('[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', line).group(0)
            bucket_end_time_datetime = datetime.datetime.strptime(bucket_end_time_string, "%Y.%m.%d %H:%M:%S")

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
            bucket_dict['checked_files'] = bucket_checked
            bucket_dict['transferred_total_files'] = bucket_transferred_total
            bucket_dict['checked_total_files'] = bucket_checked_total

            #print(bucket_dict)
            buckets_list.append(bucket_dict.copy())



last_backup_start_time_metric = Gauge('last_backup_start_time', 'Time of start last backup')
last_backup_end_time_metric = Gauge('last_backup_end_time', 'Time of end last backup')
last_backup_duration_time_metric = Gauge('last_backup_duration_time', 'Duration time of last backup')

bucket_backup_duration_times_metric = Gauge('bucket_backup_duration_times', 'Duration times of particular bucket backup', ['bucket_name'])
bucket_backup_transferred_files_metric = Gauge('bucket_backup_transferred_files', 'Number of file, transferred while bucket backup', ['bucket_name'])
bucket_backup_checked_files_metric = Gauge('bucket_backup_checked_files', 'Number of file, checked while bucket backup', ['bucket_name'])
bucket_backup_transferred_total_files_metric = Gauge('bucket_backup_transferred_total_files', 'Total number of files, needed to transfer while bucket backup', ['bucket_name'])
bucket_backup_checked_total_files_metric = Gauge('bucket_backup_checked_total_files', 'Total number of files, needed to check while bucket backup', ['bucket_name'])


start_http_server(8000)
while True:


    get_data_from_log_file()


    last_backup_duration_time_metric.set(backup_time.seconds)
    #last_backup_start_time_metric.set(backup_start_string)

    for bucket in buckets_list:
        #print(bucket['backup_time'])
        bucket_backup_duration_times_metric.labels(bucket['name']).set(bucket['backup_time'])
        bucket_backup_transferred_files_metric.labels(bucket['name']).set(bucket['transferred_files'])
        bucket_backup_checked_files_metric.labels(bucket['name']).set(bucket['checked_files'])
        bucket_backup_transferred_total_files_metric.labels(bucket['name']).set(bucket['transferred_total_files'])
        bucket_backup_checked_total_files_metric.labels(bucket['name']).set(bucket['checked_total_files'])


    time.sleep(120)
import math
import os
import sys
import commands as cmd
import requests
import shutil
import time
import json

import utils
import datahub
from logger import logger
from config import config
import csv_reader as creader


def process(baseurl, csvs, dataset_id, filepath, carname, parsedir, key):
    summaryfile = key + '_summary.json'
    summarypath = os.path.join(parsedir, summaryfile)

    totoalhumaninterv, totalcourse, totaltime = 0, 0, 0
    items = []
    for csv in csvs:
        csvpath = os.path.join(parsedir, csv)

        columns = ['vehicle_odometry.odometry', 'can_state.forward_vel', 'date_time.timestamp']
        rows = creader.read(csvpath, '\t', columns, True)
        if len(rows) == 0:
            logger.info('[SKIP] no data in csv, csv: {}, filepath: {}, carname: {}'.format(
                csvpath, filepath, carname
            ))
            continue

        length = len(rows)
        humaninterv, course, start_time, end_time = 0, 0, 0, 0
        start_time = int(rows[0]['date_time.timestamp'])
        end_time = int(rows[length - 1]['date_time.timestamp'])

        # NO_HUMAN_INTERVENTION = 0,
        # LATERAL_INTERVENTION = 1, // lateral intervention
        # BRAKING_INTERVENTION = 2, // braking intervention
        # MODE_SWITCH_INTERVENTION = 3, // mode switch intervention, maybe press mode switch button.
        
        humaninterv_values = ['1', '2', '3']
        if 'can_state.has_human_intervention' in rows[0]:
            for i in xrange(0, length):
                if rows[i]['can_state.has_human_intervention'] in humaninterv_values:
                    humaninterv = humaninterv + 1
        
        invalid_values = ['-1', 'nan', '-nan']
        if 'vehicle_odometry.odometry' in rows[0]:
            for i in xrange(length - 1, -1, -1):
                if not rows[i]['vehicle_odometry.odometry'] in invalid_values:
                    last = rows[i]['vehicle_odometry.odometry']
                    end_time = int(rows[i]['date_time.timestamp'])
                    break
            for i in xrange(0, length):
                if not rows[i]['vehicle_odometry.odometry'] in invalid_values:
                    first = rows[i]['vehicle_odometry.odometry']
                    start_time = int(rows[i]['date_time.timestamp'])
                    break
            course = float(last) - float(first)
        else:
            course = rows[0]['can_state.forward_vel']*0.1
            for i in xrange(1, length):
                try:
                    course = course + (rows[i]['can_state.forward_vel']
                                       *
                                       (rows[i]['date_time.timestamp']
                                        -
                                        rows[i-1]['date_time.timestamp']
                                        )
                                       / 1e6)
                except Exception as e:
                    logger.warning('[SKIP ROW] row {}, csv {}, filepath {}, carname{}, {}'.format(
                        i, csv, filepath, carname, str(e)
                    ))
                    continue
        if course >= 5000:
            logger.warning('[WARNING] [ReadCSV] mileage too big, {}, csv: {}, filepath: {}, carname: {}'.format(
                course, csv, filepath, carname
            ))
        if start_time >= end_time:
            logger.error('[TIME ERROR] starttime({}) greater than endtime({}), csv {}, filepath {}, carname {}'.format(
                start_time, end_time, csv, filepath, carname
            ))
            continue
        if course < 10:
            logger.info('[SKIP] mileage is {}, too small, csv: {}, filepath: {}, carname: {}'.format(
                course, csv, filepath, carname
            ))
            continue
        if course >= 10000000:
            logger.warning('[ERROR] [Caculate] mileage too big, {}, csv: {}, filepath: {}, carname: {}'.format(
                course, csv, filepath, carname
            ))
            continue
        if course == float('inf') or course == -float('inf'):
            logger.error('[SKIP] mileage is {}, csv: {}, filepath: {}, carname: {}'.format(
                course, csv, filepath, carname
            ))
            continue

        item = {
            'starttime': int(start_time / 1000),
            'endtime': int(end_time / 1000)
        }
        start_time = int(start_time / 1000000)
        end_time = int(end_time / 1000000)
        totalcourse += course
        totaltime += end_time - start_time
        item['duration'] = end_time - start_time
        item['mileage'] = int(course)
        items.append(item)

        totoalhumaninterv += humaninterv

        mileage = {}
        mileage['datasetid'] = dataset_id
        mileage['vehicle'] = carname
        mileage['filename'] = csv
        mileage['filepath'] = filepath
        mileage['start_time'] = start_time
        mileage['end_time'] = end_time
        mileage['miles'] = int(course)
        print(mileage)

        url = baseurl + 'mileages/'
        res = requests.get(url, params={
            'vehicle': carname,
            'start_time': start_time,
            'end_time': end_time
        })
        put = False
        if res.status_code == requests.codes.ok:
            resj = res.json()
            if len(resj) == 1:
                url = url + str(resj[0]['id']) + '/'
                put = True
        if put:
            res = requests.put(url, data=mileage)
        else:
            res = requests.post(url, data=mileage)

        if utils.response_success(res.status_code):
            logger.info('[SUCCESS] success to handle mileage {}'.format(str(mileage)))
        else:
            logger.error('[FAILED] fail to handle mileage {}'.format(str(mileage)))
            continue

    if totalcourse > 0 and totaltime > 0:
        datafile = {}
        datafile['datasetid'] = dataset_id
        datafile['vehicle'] = carname
        datafile['duration'] = totaltime
        datafile['miles'] = int(totalcourse)
        datafile['filename'] = os.path.basename(filepath)

        url = baseurl + 'datafiles/'
        res = requests.get(url, params={'vehicle': carname, 'datasetid': dataset_id})
        put = False
        if res.status_code == requests.codes.ok:
            resj = res.json()
            cars = resj['results']
            if len(cars) == 1:
                url = url + str(cars[0]['id']) + '/'
                put = True
        if put:
            res = requests.put(url, data=datafile)
        else:
            res = requests.post(url, data=datafile)

        if res.status_code == requests.codes.ok:
            logger.info('success to handle datafile {}'.format(str(datafile)))
        else:
            logger.error('fail to handle datafile {}'.format(str(datafile)))
    
    if len(items) > 0:
        summary = None
        with open(summarypath, 'r') as f:
            summary = json.loads(f.read())
            summary['mileage'] = int(totalcourse)
            summary['duration'] = totaltime
            summary['human_intervation'] = totoalhumaninterv
            if 'data_list' in summary and len(summary['data_list']) > 0:
                for dataitem in summary['data_list']:
                    for item in items:
                        if dataitem['starttime'] <= item['starttime'] and \
                                dataitem['endtime'] >= item['endtime']:
                            dataitem['mileage'] = item['mileage']
                            dataitem['duration'] = item['duration']
                            break
        if summary is not None:
            with open(summarypath, 'w') as f:
                f.write(json.dumps(summary, indent=4, separators=(',', ': ')))
    return True


def _remove_filepath_old_data(baseurl, filepath):
    res = requests.get(
        baseurl + 'mileages',
        params={
            'filepath': filepath
        },
        timeout=None
    )
    if res.status_code == requests.codes.ok:
        resj = res.json()
        if len(resj) > 0:
            for item in resj:
                res = requests.delete(baseurl + 'mileages/' + str(item['id']))
                if res.status_code == 204:
                    print('[DELETE SUCCESS] - {}'.format(item['filename']))
                else:
                    print('[DELETE FAILED] - {}'.format(item['filename']))
        else:
            print('[DELETE SUCCESS] - NO OLD DATA')
        return True
    else:
        print('[DELETE FAILED] - GET OLD DATA FAILED')
    return False


def execute(**kwargs):
    if 'dataset_id' not in kwargs or \
            'carname' not in kwargs or \
            'targetdir' not in kwargs or \
            'key' not in kwargs or \
            'date' not in kwargs or \
            'filepath' not in kwargs:
        logger.error('arguments error, kwargs: {}'.format(str(kwargs)))
        return False

    baseurl = config.get('CarDataUrl', '') if 'baseurl' not in kwargs else kwargs['baseurl']
    if not baseurl:
        logger.error('config error, can\'t read BaseDir or CarDataUrl')
        return False

    dataset_id = kwargs['dataset_id']
    targetdir = kwargs['targetdir']
    key = kwargs['key']
    date = kwargs['date']
    carname = kwargs['carname']
    filepath = kwargs['filepath']

    parsedir = os.path.join(targetdir, date, carname, 'parse')
    if not os.path.exists(parsedir):
        logger.error('[ERROR] parse directory {} not exits'.format(parsedir))
        return False

    csvs = []

    for csv in os.listdir(parsedir):
        if csv.endswith('.csv') and csv.startswith('csv_' + key):
            csvs.append(csv)
    if len(csvs) == 0:
        logger.info('[ERROR] no in {}'.format(filepath))
        return False
    _remove_filepath_old_data(baseurl, filepath)
    return process(
        baseurl,
        csvs,
        dataset_id,
        filepath,
        carname,
        parsedir,
        key
    )

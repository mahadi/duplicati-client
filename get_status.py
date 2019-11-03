# -*- coding: utf-8 -*-

__author__ = 'mahadi'

import yaml
import re
import logging

import requests

from subprocess import Popen, PIPE
from datetime import datetime
from pathlib import Path
from functools import reduce
from enum import Flag

from dateutil.relativedelta import relativedelta

logger = logging.getLogger('get_status')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)

# need to use absolute paths to resolve ~
PARAMETERS_FILE = Path.home() / '.config/duplicati-client/parameters.yml'
# PARAMETERS_FILE = 'parameters.yml'

BASEDIR = Path(__file__).parent
CONFIG_FILE = BASEDIR / 'config.yml'

# Result state
class Result(Flag):
    OK = 0
    NOK = 1
    PENDING = 2
    INITIAL = 4

with open(CONFIG_FILE) as f:
    config = yaml.safe_load(f)
    SERVER_IP = config["server_ip"]
    DB_HOST = config["db_host"]
    DB = config["db"]
    DB_USERNAME = config["db_username"]
    DB_PASSWORD = config["db_password"]
    PYTHON = config["python"]

DUCCMD = f"{PYTHON} {BASEDIR / 'duplicati_client.py'}"


def call(cmd):
    logger.info(f"Executing: {cmd}")
    p = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    out, err = out.decode('utf8'), err.decode('utf8')
    logger.debug(f"STDOUT {out}")
    logger.debug(f"STDERR {err}")
    return out, err


def extract_yaml(out, err):
    # print(out, err)
    # remove the first and the last 3 (verbose) lines otherwise yaml wont be parsed
    # https://github.com/yaml/pyyaml/issues/318

    lines = out.splitlines()
    if not lines:
        logger.error(f"Nothing to split here, out is '{out}'")
    # data starts at first line which starts with a dash
    for i, line in enumerate(lines):
        if line.startswith('-'):
            break
    # and ends at first empty line
    for j, line in enumerate(lines[i:]):
        if not line.strip():
            # lines is empty
            break
    data = lines[i:i+j]
    # concat with newlines again to get raw yaml
    data_str = "\n".join(data)
    # print(data_str)
    data = yaml.safe_load(data_str)
    return data


def get_backup_info(id):
    return extract_yaml(*call(f'{DUCCMD} get backup {id}'))


def verify_backup(name, backup_data):
    assert len(backup_data) == 1
    # print(backup_data)
    # planned schedule
    data = backup_data[0][name]
    repeat = data['Schedule']['Repeat']

    # @todo dont know if its necessary to combine them?
    # see duplicati/Duplicati/Server/webroot/ngax/scripts/services/AppUtils.js, function reloadTexts()
    quantifier_to_keyword = {
        'm': 'minutes',
        'h': 'hours',
        'D': 'days',
        'W': 'weeks',
        'M': 'months',
        'Y': 'years',
    }

    deltas = {}
    for repeat_part in repeat.split():
        m = re.match(r'(?P<amount>\d+)(?P<quantifier>\S)', repeat_part)
        quantifier = m.group('quantifier')
        amount = int(m.group('amount'))
        keyword = quantifier_to_keyword[quantifier]
        assert keyword not in deltas
        deltas[keyword] = amount

    NOW = datetime.now()
    limit_date_in_past = NOW - relativedelta(**deltas)

    if 'Progress' in data and data['Progress']['State'] == 'Backup_ProcessingFiles':
        result = Result.PENDING
    else:
        # check if there is one successfull run within the planned schedule time back from now
        # https://github.com/Pectojin/duplicati-client/issues/16
        # "last run is only considering successful runs. If the backup doesn't complete it's not considered run."
        last_run = datetime.strptime(data['Last run']['Started'], "%Y-%m-%d %H:%M:%S")
        if last_run > limit_date_in_past:
            result = Result.OK
        else:
            result = Result.NOK

    return result


def main():
    # set password
    # contains the server password
    call(f'{DUCCMD} params {PARAMETERS_FILE}')
    call(f'{DUCCMD} login')  # {SERVER_IP}')

    data = extract_yaml(*call(f'{DUCCMD} list backups'))
    # returns a of dictionaries, one dictionary for each backup
    # print(data)

    logger.info(f"{len(data)} backups found")

    results = {}
    for backup in data:
        for name, info in backup.items():
            backup_data = get_backup_info(info['ID'])
            status = verify_backup(name, backup_data)
            # prevent possible overwriting of statuses
            assert name not in results
            results[name] = status

    logger.info(results)

    call(f'{DUCCMD} logout')


    # binary or to all results
    combined_status = reduce(lambda x, y: x | y, results.values())

    overall_status = Result.INITIAL.value
    if all(v == Result.OK for v in results.values()):
        overall_status = Result.OK.value
    else:
        priorities = [
            Result.PENDING,
            Result.NOK
        ]
        # the last element in the priority list has the highest prio
        # -> if the bit is set, the overall status will take its value
        for p in priorities:
            if combined_status & p:
                overall_status = p.value

    # use integer values for influxdb (and grafana visualization)
    # overall_status = 1 if all(v == 'OK' for v in results.values()) else 0
    text_map = {
        0: "NOK",
        1: "OK",
        2: "PENDING",
        4: "INITIAL",  # was never run before
    }
    logger.info(f"Overall status: {text_map[overall_status]}")

    # put status to influx
    response = requests.post(
        f'http://{DB_HOST}:8086/write?db={DB}&precision=s',
        auth=(DB_USERNAME, DB_PASSWORD),
        data=f'duplicati_overall_backup_status value={overall_status}'
    )
    logger.debug(response.status_code)
    if response.status_code in [requests.codes.ok, requests.codes.no_content]:
        logger.info("Result saved")
    else:
        logger.error(response.content)
        response.raise_for_status()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import sys
import subprocess


def main(siri_etl_commit):
    with open('requirements-docker.txt') as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('-r requirements.txt'):
                line = '-r https://github.com/hasadna/open-bus-siri-etl/raw/{}/requirements.txt'.format(siri_etl_commit)
            print(line)
    print('https://github.com/hasadna/open-bus-siri-etl/archive/{}.zip'.format(siri_etl_commit))


if __name__ == '__main__':
    main(*sys.argv[1:])

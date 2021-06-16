#!/usr/bin/env python3
import sys
import subprocess


def main(siri_requester_commit):
    new_lines = []
    with open('requirements-docker.txt') as f:
        for line in f.readlines():
            if line.startswith('-r https://github.com/hasadna/open-bus-siri-requester/raw/'):
                line = '-r https://github.com/hasadna/open-bus-siri-requester/raw/{}/requirements.txt\n'.format(siri_requester_commit)
            elif line.startswith('https://github.com/hasadna/open-bus-siri-requester/archive/'):
                line = 'https://github.com/hasadna/open-bus-siri-requester/archive/{}.zip\n'.format(siri_requester_commit)
            new_lines.append(line)
    with open('requirements-docker.txt', 'w') as f:
        f.writelines(new_lines)
    subprocess.check_call(['git', 'add', 'requirements-docker.txt'])


if __name__ == '__main__':
    main(*sys.argv[1:])

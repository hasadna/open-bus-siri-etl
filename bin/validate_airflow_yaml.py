#!/usr/bin/env python3
import sys
from ruamel import yaml


def main(airflow_yaml_filename):
    with open(airflow_yaml_filename) as f:
        airflow = yaml.safe_load(f)
    all_dag_names = []
    for dag_file in airflow.get('dag_files', []):
        with open(dag_file) as f:
            for dag in yaml.safe_load(f):
                assert dag['name'] not in all_dag_names
                all_dag_names.append(dag['name'])
    print('OK. found {} dags: {}'.format(len(all_dag_names), ', '.join(all_dag_names)))


if __name__ == '__main__':
    main(*sys.argv[1:])

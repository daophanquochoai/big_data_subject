"""Enable only the new training dispatcher after the existing Airflow is ready."""
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def enable():
    base = 'http://localhost:8080'
    for attempt in range(60):
        try:
            with urlopen(base+'/auth/token', timeout=5) as response:
                token = json.load(response)['access_token']
            headers = {'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'}
            path = base+'/api/v2/dags/single_node_rul_training'
            with urlopen(Request(path, headers=headers), timeout=5) as response:
                dag = json.load(response)
            if dag.get('has_import_errors'):
                raise RuntimeError('Training DAG has import errors')
            if dag['is_paused']:
                with urlopen(Request(path, data=b'{"is_paused":false}', headers=headers, method='PATCH'), timeout=5) as response:
                    json.load(response)
            print('single_node_rul_training is enabled; application schedule remains opt-in.')
            return
        except (HTTPError, URLError, OSError):
            time.sleep(2)
    raise RuntimeError('Airflow training DAG did not become ready within two minutes')


if __name__ == '__main__':
    enable()

import json
from pathlib import Path
import sys
import io
import tarfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import runner


class DockerFake:
    def __init__(self, state=None):
        self.state = state
        self.commands = []

    def __call__(self, args, **kwargs):
        self.commands.append(args)
        if args[:2] == ['container', 'inspect']:
            if self.state is None:
                raise runner.DockerError('No such container', not_found=True)
            return json.dumps([{'Id': 'container-id', 'Config': {'Labels': {'pdm.run-key': 'manual:test', 'pdm.config': runner.config_hash(self.run)}}, 'State': self.state}])
        if args[:2] == ['container', 'create']:
            return 'container-id'
        return ''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.run = {'id': 1, 'run_key': 'manual:test', 'device': 'cpu', 'world_size': 2,
                    'epochs': 3, 'batch_size': 32, 'learning_rate': 0.02, 'training_image': 'training:test'}
        DockerFake.run = self.run

    def test_names_are_stable_and_safe_for_arbitrary_run_keys(self):
        name = runner.container_name(self.run)
        self.assertEqual(name, runner.container_name(self.run))
        self.assertNotIn(':', name)
        self.assertNotIn('/', runner.artifact_directory({**self.run, 'run_key': '../../secret'}))

    def test_existing_running_container_is_attached_without_create_or_start(self):
        docker = DockerFake({'Status': 'running', 'Running': True, 'ExitCode': 0})
        container_id = runner.ensure_container(self.run, docker, image='training:test')
        self.assertEqual(container_id, 'container-id')
        self.assertEqual(len(docker.commands), 1)

    def test_new_gpu_container_requests_devices_and_single_node_process_count(self):
        docker = DockerFake()
        runner.ensure_container({**self.run, 'device': 'cuda', 'world_size': 4}, docker, image='training:test')
        create = next(c for c in docker.commands if c[:2] == ['container', 'create'])
        self.assertIn('--gpus', create)
        self.assertIn('--nproc-per-node=4', create)
        self.assertIn('--nnodes=1', create)
        self.assertIn('--standalone', create)
        self.assertEqual(docker.commands[-1][0:2], ['container', 'start'])

    def test_cpu_job_does_not_request_gpu_devices(self):
        docker = DockerFake()
        runner.ensure_container(self.run, docker, image='training:test')
        create = next(c for c in docker.commands if c[:2] == ['container', 'create'])
        self.assertNotIn('--gpus', create)

    def test_completed_container_is_not_restarted_on_retry(self):
        docker = DockerFake({'Status': 'exited', 'Running': False, 'ExitCode': 0})
        runner.ensure_container(self.run, docker, image='training:test')
        self.assertEqual(len(docker.commands), 1)

    def test_cancel_does_not_report_cancelled_until_stop_and_confirmation(self):
        calls = []
        def docker(args, **kwargs):
            calls.append(args)
            if args[:2] == ['container', 'inspect']:
                return json.dumps([{'State': {'Running': False}}])
            return ''
        runner.stop_container('train-test', docker)
        self.assertEqual(calls[0][0:2], ['container', 'stop'])
        self.assertEqual(calls[1][0:2], ['container', 'inspect'])

    def test_structured_logs_are_parsed_and_noise_is_not_a_metric(self):
        logs = 'ordinary torch log\nTRAINING_EVENT {"completed_epochs":1,"metrics":[{"epoch":1,"loss":0.5,"accuracy":0.8,"elapsed_seconds":1}]}\n'
        events = runner.parse_events(logs)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['completed_epochs'], 1)

    def test_two_rank_frames_on_same_log_line_are_both_preserved(self):
        logs = 'TRAINING_EVENT {"ranks":[{"rank":0,"epoch":24}]}TRAINING_EVENT {"ranks":[{"rank":1,"epoch":24}]}\n'
        events = runner.parse_events(logs)
        self.assertEqual([event['ranks'][0]['rank'] for event in events], [0, 1])

    def test_create_timeout_reconciles_container_instead_of_starting_another(self):
        class AcceptedButTimedOut(DockerFake):
            def __call__(inner, args, **kwargs):
                if args[:2] == ['container', 'create']:
                    inner.state = {'Status': 'running', 'Running': True, 'ExitCode': 0}
                    raise runner.subprocess.TimeoutExpired('docker', 30)
                return super().__call__(args, **kwargs)
        docker = AcceptedButTimedOut()
        self.assertEqual(runner.ensure_container(self.run, docker), 'container-id')
        self.assertFalse(any(c[:2] == ['container', 'start'] for c in docker.commands))

    def test_cancel_between_read_and_starting_callback_never_creates_container(self):
        class CancelDuringClaim:
            def request(inner, path):
                return {'status': 'queued', 'cancel_requested': False}
            def update(inner, run_id, event):
                return {'status': 'cancelled', 'cancel_requested': True}
        docker = DockerFake()
        runner.run_training(self.run, CancelDuringClaim(), docker)
        self.assertFalse(any(c[:2] in (['container', 'create'], ['container', 'start']) for c in docker.commands))

    def test_missing_checkpoint_cannot_be_reported_successfully(self):
        def docker(args, **kwargs):
            raise runner.DockerError('No such file')
        with self.assertRaises(runner.DockerError):
            runner.verify_artifacts(self.run, 'test-container', docker)

    def test_checkpoint_and_result_are_read_from_actual_container_archive(self):
        def archive(name, data):
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode='w') as tar:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
            return output.getvalue()
        def docker(args, **kwargs):
            if 'checkpoint.pt' in args[-2]:
                return archive('checkpoint.pt', b'real-checkpoint-bytes')
            return archive('result.json', json.dumps({'epochs': 3, 'world_size': 2, 'device': 'cpu', 'dataset': 'cmapss-rul-33f-v1'}).encode())
        self.assertTrue(runner.verify_artifacts(self.run, 'test-container', docker).endswith('/checkpoint.pt'))
        with self.assertRaisesRegex(runner.DockerError, 'fingerprint'):
            runner.verify_artifacts({**self.run, 'dataset_fingerprint': 'different-snapshot'}, 'test-container', docker)

    def test_exit_failure_reports_failure_and_not_success(self):
        self.assertEqual(runner.terminal_status({'ExitCode': 1, 'OOMKilled': False}), 'failed')
        self.assertEqual(runner.terminal_status({'ExitCode': 0, 'OOMKilled': False}), 'succeeded')

    def test_unresolved_submission_never_releases_host_as_failed(self):
        updates = []
        class ApiFake:
            def request(inner, path):
                return {'status': 'starting', 'cancel_requested': False}
            def update(inner, run_id, event):
                updates.append(event)
                return {'status': 'starting', 'cancel_requested': False}
        def docker(args, **kwargs):
            raise runner.subprocess.TimeoutExpired('docker', 30)
        with self.assertRaises(runner.subprocess.TimeoutExpired):
            runner.run_training(self.run, ApiFake(), docker)
        self.assertFalse(any(event.get('status') in ('failed', 'cancelled') for event in updates))

    def test_existing_container_with_changed_config_is_rejected(self):
        docker = DockerFake({'Status': 'running', 'Running': True, 'ExitCode': 0})
        with self.assertRaises(runner.DockerError):
            runner.ensure_container({**self.run, 'epochs': 4}, docker)

    def test_initial_cancel_request_stops_existing_workload_before_terminal_callback(self):
        calls = []
        class ApiFake:
            def request(inner, path):
                return {'status': 'starting', 'cancel_requested': True}
            def update(inner, run_id, event):
                calls.append(('callback', event))
                return {'status': 'cancelled', 'cancel_requested': True}
        def docker(args, **kwargs):
            calls.append(('docker', args))
            if args[:2] == ['container', 'inspect']:
                running = not any(c[0] == 'docker' and c[1][:2] == ['container', 'stop'] for c in calls)
                return json.dumps([{'State': {'Running': running}}])
            return ''
        runner.run_training(self.run, ApiFake(), docker)
        self.assertEqual(calls[-1][0], 'callback')
        self.assertTrue(any(c[0] == 'docker' and c[1][:2] == ['container', 'stop'] for c in calls))

    def test_cancel_stop_failure_does_not_release_host(self):
        callbacks = []
        class ApiFake:
            def request(inner, path):
                return {'status': 'starting', 'cancel_requested': True}
            def update(inner, run_id, event):
                callbacks.append(event)
        def docker(args, **kwargs):
            if args[:2] == ['container', 'inspect']:
                return json.dumps([{'State': {'Running': True}}])
            raise runner.DockerError('Docker unavailable while stopping')
        with self.assertRaises(runner.DockerError):
            runner.run_training(self.run, ApiFake(), docker)
        self.assertEqual(callbacks, [])

    def test_success_requires_completed_telemetry_for_every_rank(self):
        updates = []
        class ApiFake:
            def request(inner, path):
                return {'status': 'starting', 'cancel_requested': False}
            def update(inner, run_id, event):
                updates.append(event)
                return {'status': event.get('status', 'running'), 'cancel_requested': False}
        inspected = 0
        def docker(args, **kwargs):
            nonlocal inspected
            if args[:2] == ['container', 'inspect']:
                inspected += 1
                if inspected == 1:
                    raise runner.DockerError('No such container', not_found=True)
                return json.dumps([{'State': {'Status': 'exited', 'Running': False, 'ExitCode': 0}}])
            if args[:2] == ['container', 'logs']:
                return 'TRAINING_EVENT ' + json.dumps({'completed_epochs': 3, 'artifact_path': '/checkpoint.pt',
                    'ranks': [{'rank': 0, 'epoch': 3, 'status': 'succeeded'}]})
            return 'container-id'
        with patch('runner.verify_artifacts', return_value='/checkpoint.pt'):
            with self.assertRaises(RuntimeError):
                runner.run_training(self.run, ApiFake(), docker)
        self.assertEqual(updates[-1]['status'], 'failed')


if __name__ == '__main__':
    unittest.main()

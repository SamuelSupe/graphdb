import ast
import unittest
from pathlib import Path


# Load the gate's deployment behavior without starting Docker or requiring images.
source = Path(__file__).with_name('raft_rolling_gate.py')
deployment_class = next(node for node in ast.parse(source.read_text()).body
                        if isinstance(node, ast.ClassDef) and node.name == 'Deployment')
namespace = {}
exec(compile(ast.Module(body=[deployment_class], type_ignores=[]), str(source), 'exec'), namespace)


class RollingCatchupTest(unittest.TestCase):
    def deployment(self):
        deployment = namespace['Deployment'].__new__(namespace['Deployment'])
        deployment.catchup_barriers = {}
        deployment.node = lambda group, node: node
        self.position = {'node_id': 1, 'leader_id': 1, 'term': 1, 'commit_index': 10}
        namespace['expect'] = lambda *args, **kwargs: self.position.copy()
        self.follower_applied = 8
        self.node_error = {}
        deployment.statuses = lambda group: [
            {**self.position, 'applied_index': self.position['commit_index']},
            {**self.position, 'node_id': 2, 'applied_index': self.follower_applied, **self.node_error},
        ]
        return deployment

    def test_progressing_follower_reaches_committed_barrier_under_continuing_writes(self):
        deployment = self.deployment()
        result = False
        for _ in range(4):
            result = deployment.caught_up({'cluster_id': 'test'})
            if result:
                break
            self.position['commit_index'] += 1
            self.follower_applied += 1
        self.assertTrue(result, 'a moving head hid completed application of the committed barrier')
        self.assertGreater(self.position['commit_index'], self.follower_applied)
        # The next wait must capture a new barrier rather than reuse this success.
        self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))

    def test_stopped_follower_never_passes(self):
        deployment = self.deployment()
        for _ in range(4):
            self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))
            self.position['commit_index'] += 1

    def test_initial_empty_head_does_not_pin_an_unusable_barrier(self):
        deployment = self.deployment()
        self.position['commit_index'] = 0
        self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))
        self.position['commit_index'] = self.follower_applied = 10
        self.assertTrue(deployment.caught_up({'cluster_id': 'test'}))

    def test_new_leader_or_term_requires_a_new_barrier(self):
        for leader in [1, 2]:
            with self.subTest(leader=leader):
                deployment = self.deployment()
                self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))
                self.position.update(leader_id=leader, term=2, commit_index=20)
                self.follower_applied = 10
                self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))
                self.follower_applied = 20
                self.assertTrue(deployment.caught_up({'cluster_id': 'test'}))

    def test_failed_or_disagreeing_replica_never_passes(self):
        for error in [{'error': 'apply failed'}, {'snapshot_error': 'snapshot failed'}, {'leader_id': 2}]:
            with self.subTest(error=error):
                deployment = self.deployment()
                self.follower_applied = 10
                self.node_error = error
                self.assertFalse(deployment.caught_up({'cluster_id': 'test'}))


if __name__ == '__main__':
    unittest.main()

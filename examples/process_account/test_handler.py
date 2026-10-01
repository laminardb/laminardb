"""Batch arithmetic, replay purity and error boundaries for the account example."""

from pathlib import Path
import unittest

import pyarrow as pa

from laminardb_process import Activation, Mutation, TimerSet, ValueState
from laminardb_process.contract import Manifest
from handler import handle


MANIFEST = Manifest.from_bytes(Path(__file__).with_name("manifest.json").read_bytes())


def input_activation(id, account, amount, at_us, prior=None):
    batch = pa.record_batch([
        pa.array([account], type=pa.utf8()),
        pa.array([amount], type=pa.int64()),
        pa.array([at_us], type=pa.timestamp("us")),
    ], schema=MANIFEST.input_schema)
    return Activation(id, account.encode(), account, at_us, batch, None,
                      ValueState(prior is not None, prior))


class AccountActivityTests(unittest.TestCase):
    def test_mixed_batch_matches_independent_reference(self):
        results = {result.activation_id: result for result in handle((
            input_activation(10, "alice", 25, 101_000, 80),
            Activation(11, b"charlie", "charlie", 110_500, None, "inactive", ValueState(True, 7)),
            input_activation(12, "bob", 10, 112_000, 40),
        ))}
        self.assertEqual(set(results), {10, 11, 12})
        expected = {
            10: ("alice", "threshold", 105, 101_000, Mutation.set(105), (TimerSet("inactive", 111_000),)),
            11: ("charlie", "inactive", 7, 110_500, Mutation(), ()),
            12: ("bob", "running", 50, 112_000, Mutation.set(50), (TimerSet("inactive", 122_000),)),
        }
        for id, result in results.items():
            self.assertEqual(len(result.output), 1)
            row = result.output[0]
            self.assertEqual(row.schema, MANIFEST.output_schema)
            actual = (row.column(0)[0].as_py(), row.column(1)[0].as_py(),
                      row.column(2)[0].as_py(), row.column(3).cast(pa.int64())[0].as_py(),
                      result.mutation, result.timers)
            self.assertEqual(actual, expected[id])

    def test_repeated_logical_activation_has_no_worker_state(self):
        activation = input_activation(17, "alice", 25, 101_000, 80)
        first = handle((activation,))[0]
        handle((input_activation(18, "alice", 999, 102_000),))
        repeated = handle((activation,))[0]
        self.assertEqual(first.mutation, Mutation.set(105))
        self.assertEqual(first.mutation, repeated.mutation)
        self.assertEqual(first.timers, repeated.timers)
        self.assertTrue(first.output[0].equals(repeated.output[0]))
        fresh = handle((input_activation(19, "alice", 1, 103_000),))[0]
        self.assertEqual(fresh.mutation, Mutation.set(1))

    def test_checked_int64_arithmetic(self):
        maximum = (1 << 63) - 1
        for activation in (
            input_activation(1, "alice", 1, 100_000, maximum),
            input_activation(2, "bob", 0, maximum - 9_999),
        ):
            with self.subTest(id=activation.id), self.assertRaises(pa.ArrowInvalid):
                handle((activation,))
        timer = Activation(3, b"host-key", "alice", maximum, None, "inactive", ValueState(True, 7))
        result = handle((timer,))[0]
        self.assertEqual(result.mutation, Mutation())
        self.assertEqual(result.timers, ())
        self.assertEqual(result.output[0].column(3).cast(pa.int64())[0].as_py(), maximum)

    def test_invalid_state_and_callbacks_are_rejected(self):
        for activation in (
            Activation(1, b"host-key", "alice", 100_000, None, None, ValueState(False)),
            Activation(2, b"host-key", "alice", 100_000, None, "other", ValueState(True, 1)),
            Activation(3, b"host-key", "alice", 100_000, None, "inactive", ValueState(True, None)),
        ):
            with self.subTest(id=activation.id), self.assertRaises(ValueError):
                handle((activation,))


if __name__ == "__main__":
    unittest.main()

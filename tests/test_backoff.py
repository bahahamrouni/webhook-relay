import unittest

from webhook_relay.backoff import backoff_ceiling, compute_delay


class _Max:
    def uniform(self, a, b):
        return b


class _Min:
    def uniform(self, a, b):
        return a


class BackoffTests(unittest.TestCase):
    def test_ceiling_doubles_then_caps(self):
        ceilings = [backoff_ceiling(n, base=2, cap=300) for n in range(1, 10)]
        self.assertEqual(ceilings, [2, 4, 8, 16, 32, 64, 128, 256, 300])

    def test_delay_is_within_zero_and_ceiling(self):
        self.assertEqual(compute_delay(3, 2, 300, rng=_Max()), 8)
        self.assertEqual(compute_delay(3, 2, 300, rng=_Min()), 0)
        for _ in range(200):
            self.assertTrue(0 <= compute_delay(4, 2, 300) <= 16)

    def test_attempt_must_be_positive(self):
        with self.assertRaises(ValueError):
            backoff_ceiling(0, 2, 300)


if __name__ == "__main__":
    unittest.main()

import unittest

from webhook_relay.signing import SignatureError, sign_header, verify_signature

SECRET = "s3cret-value-for-tests"
BODY = b'{"id":"evt_1"}'


class SigningTests(unittest.TestCase):
    def test_round_trip(self):
        header = sign_header(SECRET, BODY, timestamp=1_000)
        self.assertEqual(verify_signature(SECRET, header, BODY, now=1_000), 1_000)

    def test_wrong_secret_fails(self):
        header = sign_header(SECRET, BODY, timestamp=1_000)
        with self.assertRaisesRegex(SignatureError, "mismatch"):
            verify_signature("another-secret-value", header, BODY, now=1_000)

    def test_tampered_body_fails(self):
        header = sign_header(SECRET, BODY, timestamp=1_000)
        with self.assertRaisesRegex(SignatureError, "mismatch"):
            verify_signature(SECRET, header, BODY + b" ", now=1_000)

    def test_tampered_timestamp_fails(self):
        header = sign_header(SECRET, BODY, timestamp=1_000)
        forged = header.replace("t=1000", "t=1001")
        with self.assertRaisesRegex(SignatureError, "mismatch"):
            verify_signature(SECRET, forged, BODY, now=1_001)

    def test_replay_outside_tolerance_fails(self):
        header = sign_header(SECRET, BODY, timestamp=1_000)
        with self.assertRaisesRegex(SignatureError, "tolerance"):
            verify_signature(SECRET, header, BODY, tolerance=300, now=1_000 + 301)
        self.assertEqual(verify_signature(SECRET, header, BODY, tolerance=300, now=1_300), 1_000)

    def test_malformed_headers_fail(self):
        for header in ["", "garbage", "t=abc,v1=00", "v1=00", "t=1000"]:
            with self.subTest(header=header), self.assertRaisesRegex(SignatureError, "malformed"):
                verify_signature(SECRET, header, BODY, now=1_000)


if __name__ == "__main__":
    unittest.main()

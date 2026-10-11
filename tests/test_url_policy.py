import unittest

from webhook_relay.url_policy import UnsafeTarget, validate_target


class UrlPolicyTests(unittest.TestCase):
    def test_blocks_internal_destinations(self):
        for url in [
            "http://127.0.0.1:8080/hook",
            "http://10.0.0.5/hook",
            "http://192.168.1.10/hook",
            "http://172.16.0.1/hook",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]/hook",
            "http://[::ffff:127.0.0.1]/hook",
            "http://0.0.0.0/hook",
            "http://localhost/hook",
        ]:
            with self.subTest(url=url), self.assertRaises(UnsafeTarget):
                validate_target(url)

    def test_blocks_bad_schemes_and_shapes(self):
        for url in [
            "ftp://8.8.8.8/x",
            "file:///etc/passwd",
            "http:///nohost",
            "http://user:pw@8.8.8.8/x",
            "http://8.8.8.8:99999/x",
            "not a url",
        ]:
            with self.subTest(url=url), self.assertRaises(UnsafeTarget):
                validate_target(url)

    def test_allows_public_address_literals(self):
        validate_target("http://8.8.8.8/hook")
        validate_target("https://1.1.1.1:8443/hook")

    def test_private_allowed_only_when_enabled(self):
        validate_target("http://127.0.0.1:9000/hook", allow_private=True)
        with self.assertRaises(UnsafeTarget):
            validate_target("ftp://127.0.0.1/hook", allow_private=True)


if __name__ == "__main__":
    unittest.main()

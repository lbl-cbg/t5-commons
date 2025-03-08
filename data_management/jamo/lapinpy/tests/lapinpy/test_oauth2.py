import unittest
from lapinpy.oauth2 import start_session
from lapinpy import oauth2
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, ANY, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, ANY, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestOauth2(unittest.TestCase):

    @patch.object(oauth2, 'OAuth2Session')
    def test_start_session(self, session):
        start_session('my_id', 'my_private_key', 'my_token_url')

        session.assert_called_with('my_id', 'my_private_key', ANY, grant_type='client_credentials',
                                   token_endpoint='my_token_url')
        self.assertIn(call().fetch_token(), session.mock_calls)


if __name__ == '__main__':
    unittest.main()

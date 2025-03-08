### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import unittest
from lapinpy.apps.lapin_doc import Lapin
import sys
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, mock_open
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, mock_open
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestLapinDoc(unittest.TestCase):

    @patch('builtins.open' if sys.version_info[0] >= 3 else '__builtin__.open', new_callable=mock_open,
           read_data='foo bar')
    def test_Lapin_get_docs(self, mock_open):
        lapin = Lapin()
        self.assertEqual(lapin.get_docs([], {}), 'foo bar')


if __name__ == '__main__':
    unittest.main()

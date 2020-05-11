import unittest
from unittest.mock import MagicMock, create_autospec
from modules.detail_lote_site.detaillotesite import getdetaillotesite

class TestDetalleLoteSite(unittest.TestCase):
    
    
    def test_get_detalle_lote(self):
        value = [(479, 'A', 682591529, "", "", "", "", "", ""), (479, 'A', 806940750, "", "", "", "", "", "")]
        mock_function = create_autospec(getdetaillotesite, return_value=value)
        mock_function(1)

        self.assertIsNotNone(mock_function(1))
        mock_function.assert_called_once_with(1, 2, 3)
        mock_function.assert_called_with(1, 2, 3, test='wow')

if __name__ == '__main__':
    unittest.main()
"""Missing image dependencies must fail before collection begins."""
import builtins
import unittest
from unittest.mock import patch
from product_collector.__main__ import main


class DependencyTests(unittest.TestCase):
    def test_missing_pillow_is_reported_before_opening_collection(self):
        original_import = builtins.__import__
        def importing(name, *args, **kwargs):
            if name == 'media':
                raise ModuleNotFoundError("No module named 'PIL'", name='PIL')
            return original_import(name, *args, **kwargs)
        with patch('sys.argv', ['collector', '--download-images']), \
             patch('builtins.__import__', side_effect=importing), \
             patch('product_collector.__main__.open_collection') as opening, \
             patch('sys.stderr') as stderr:
            with self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)
            opening.assert_not_called()
            self.assertIn('PIL', ''.join(call.args[0] for call in stderr.write.call_args_list))

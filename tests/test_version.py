import gestalt
import unittest


class VersionTestCase(unittest.TestCase):
    """ Basic test cases """

    def test_version(self):
        """ check gestalt exposes a version attribute """
        self.assertTrue(hasattr(gestalt, "__version__"))
        self.assertIsInstance(gestalt.__version__, str)


if __name__ == "__main__":
    unittest.main()

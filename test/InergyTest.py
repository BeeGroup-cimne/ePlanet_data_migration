import unittest

from dotenv import load_dotenv

from Inergy import InergySource


class SourceTesting(unittest.TestCase):
    token = ""

    def setUp(self):
        load_dotenv()

    def test_1_get_credentials(self):
        res = InergySource.InergySource.authenticate()
        self.assertIsInstance(res, dict)
        self.assertIsNotNone(res.get('access_token'))
        self.__class__.token = res.get('access_token')

    def test_2_generate_element(self):
        InergySource.InergySource.insert_elements(self.token)


if __name__ == '__main__':
    unittest.main()

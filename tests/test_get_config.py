import os
import tempfile
import unittest

from src.utils import get_config


class TestGetConfig(unittest.TestCase):

    def setUp(self):
        config_content = """
        data_sets_url: "https://datasets.imdbws.com"
        dataset_paths:
            title: "name.basics.tsv.gz"
            name: "name.basics.tsv.gz"
            principals: "title.principals.tsv.gz"
            ratings: "title.ratings.tsv.gz"
        """
        _, self.config_path = tempfile.mkstemp()
        with open(self.config_path, 'w') as cfg:
            cfg.write(config_content)

    def tearDown(self):
        os.remove(self.config_path)

    def test_get_config(self):
        config = get_config(self.config_path)
        self.assertEqual(config['data_sets_url'], "https://datasets.imdbws.com")
        self.assertEqual(len(config['dataset_paths']), 4)

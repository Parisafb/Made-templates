import unittest
import os
import sqlite3
from pipeline import DataPipeline

class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test environment before running any tests."""
        cls.db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'climate_health.sqlite')
        
        # Ensure the database file is removed before running tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        
        # Run the main function to execute the data pipeline once for all tests
        pipeline = DataPipeline(
            temp_url='https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data/download', 
            health_url='https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease', 
            db_name='climate_health'
        )
        pipeline.run()

        # Set up the database connection and cursor for reuse in tests
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

    def test_database_creation(self):
        """Test if the database file is created."""
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

    def test_table_creation(self):
        """Test if the necessary tables are created."""
        tables = ['TemperatureData', 'HealthData']
        for table in tables:
            with self.subTest(table=table):
                self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                self.assertIsNotNone(self.cursor.fetchone(), f"Table '{table}' was not created in the database.")

    def test_temperature_table_schema(self):
        """Test the schema of the TemperatureData table."""
        self.cursor.execute("PRAGMA table_info(TemperatureData);")
        columns = {column[1]: column[2] for column in self.cursor.fetchall()}
        expected_columns = {
            'Year': 'INTEGER',
            'Country': 'TEXT',
            'AverageAnnualTemperature': 'REAL',
            'AverageAnnualAnomaly': 'REAL'
        }
        self.assertDictEqual(columns, expected_columns, "Temperature data table schema is incorrect.")

    def test_health_table_schema(self):
        """Test the schema of the HealthData table."""
        self.cursor.execute("PRAGMA table_info(HealthData);")
        columns = {column[1]: column[2] for column in self.cursor.fetchall()}
        expected_columns = {
            'Country': 'TEXT',
            'Year': 'INTEGER',
            'MortalityRate': 'REAL'
        }
        self.assertDictEqual(columns, expected_columns, "Health data table schema is incorrect.")

    def test_temperature_table_data(self):
        """Test that the TemperatureData table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM TemperatureData;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Temperature data table does not contain any data.")

    def test_health_table_data(self):
        """Test that the HealthData table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM HealthData;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Health data table does not contain any data.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        cls.conn.close()
        # Ensure the database file is removed after all tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()

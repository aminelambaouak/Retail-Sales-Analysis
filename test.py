import script
import pytest


class RetailTest():
    def test_etl(self):
        self.assertEqual(script.load_and_transform_sales_data(script.spark, csv_path='/home/amine/Retail-Sales-Analysis/sales_data_sample.csv'))






if __name__ == '__main__':
    pytest.main()


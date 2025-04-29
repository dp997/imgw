import unittest

import pyarrow as pa

from imgw.extract.helpers.extract import ImgwCsv, ImgwZip, _save_failed_file, parse_table, unzip

TEST_VALID_SCHEMA = {"test_table": {"column1": pa.int64(), "column2": pa.string()}}

TEST_VALID_ZIP = b'PK\x03\x04\n\x00\x00\x00\x00\x00\x9a\xa0\x96Za\xe4\x84"\x0c\x00\x00\x00\x0c\x00\x00\x00\x08\x00\x1c\x00test.csvUT\t\x00\x03D\xda\x07hD\xda\x07hux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04\xe8\x03\x00\x00test_contentPK\x01\x02\x1e\x03\n\x00\x00\x00\x00\x00\x9a\xa0\x96Za\xe4\x84"\x0c\x00\x00\x00\x0c\x00\x00\x00\x08\x00\x18\x00\x00\x00\x00\x00\x01\x00\x00\x00\xa4\x81\x00\x00\x00\x00test.csvUT\x05\x00\x03D\xda\x07hux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04\xe8\x03\x00\x00PK\x05\x06\x00\x00\x00\x00\x01\x00\x01\x00N\x00\x00\x00N\x00\x00\x00\x00\x00'  # zip with empty test.csv file


class TestHelpers(unittest.TestCase):
    def test_imgw_csv_model(self):
        imgw_csv = ImgwCsv(filename="test.csv", content=b"test_content")
        self.assertEqual(imgw_csv.filename, "test.csv")
        self.assertEqual(imgw_csv.content, b"test_content")

    def test_imgw_zip_model(self):
        imgw_zip = ImgwZip(filename="test.zip", content=b"test_content")
        self.assertEqual(imgw_zip.filename, "test.zip")
        self.assertEqual(imgw_zip.content, b"test_content")

    def test_save_failed_file(self):
        imgw_csv = ImgwCsv(filename="test.csv", content=b"test_content")
        _save_failed_file(imgw_csv)

    # Verify that the file is created on disk

    def test_read_table_valid_csv(self):
        csv_content = b"1,test 1\n2,test 2"
        imgw_csv = ImgwCsv(filename="test_table_92734.csv", content=csv_content)
        table, table_type = parse_table(imgw_csv, schemas=TEST_VALID_SCHEMA)
        self.assertIsNotNone(table)
        self.assertEqual(table_type, "test_table")  # Assuming "table" is a valid table type

    def test_read_table_invalid_csv(self):
        csv_content = b"invalid csv content"
        imgw_csv = ImgwCsv(filename="test_table.csv", content=csv_content)
        table, table_type = parse_table(imgw_csv)
        self.assertIsNone(table)
        self.assertEqual(table_type, "")

    def test_read_table_invalid_table_type(self):
        csv_content = b"column1,column2\n1,2\n3,4"
        imgw_csv = ImgwCsv(filename="invalid_table.csv", content=csv_content)
        table, table_type = parse_table(imgw_csv)
        self.assertIsNone(table)
        self.assertEqual(table_type, "")

    def test_unzip_valid_zip(self):
        zip_content = TEST_VALID_ZIP
        imgw_zip = ImgwZip(filename="test.zip", content=zip_content)
        csv_files = unzip(imgw_zip)
        self.assertIsInstance(csv_files, list)
        self.assertIsInstance(csv_files[0], ImgwCsv)

    def test_unzip_invalid_zip(self):
        zip_content = b"invalid zip content"
        imgw_zip = ImgwZip(filename="test.zip", content=zip_content)
        csv_files = unzip(imgw_zip)
        self.assertEqual(csv_files, [])

    # dont know how to mock dlt.sources.helpers.requests
    # @patch("requests.get")
    # def test_fetch_data_valid_url(self, mock_get):
    #     mock_response = Mock()
    #     mock_response.status_code = 200
    #     mock_response.content = b"test_content"
    #     mock_get.return_value = mock_response
    #     imgw_zip = fetch_zip_data("https://example.com/subdir/test.zip")
    #     self.assertEqual(imgw_zip.filename, "test.zip")
    #     self.assertEqual(imgw_zip.content, b"test_content")
    #
    # @patch("requests.get")
    # def test_fetch_data_invalid_url(self, mock_get):
    #     mock_get.side_effect = requests.RequestException("Mocked request exception")
    #     imgw_zip = fetch_zip_data("https://example.com/invalid_url.zip")
    #     self.assertEqual(imgw_zip.filename, "")
    #     self.assertEqual(imgw_zip.content, b"")
    #
    # @patch("requests.get")
    # def test_get_json_data_404(self, mock_get):
    #     mock_response = Mock()
    #     mock_response.status_code = 404
    #     mock_response.json.return_value = {"status": False, "message": "No products were found"}
    #     mock_get.return_value = mock_response
    #     mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
    #
    #     result = list(get_json_data("test_path"))
    #     self.assertEqual(result, [{}])


if __name__ == "__main__":
    unittest.main()

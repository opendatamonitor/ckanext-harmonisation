from ckanext.harmonisation.lib.find_format import find_format

class TestFindFormat(object):

    def test_valid_format(self):
        assert find_format("http://localhost/test.zip") == "zip"

    def test_invalid_format(self):
        assert find_format("http://localhost") == ""

    def test_unknown_format(self):
        assert find_format("http://localhost/test.sql") == ""

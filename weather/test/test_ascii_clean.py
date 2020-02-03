from weather.utilities.ascii_clean import create_ascii_str_from_str

def test_ascii_clean():
    string = 'Eftasåsen4'
    expected = 'eftas-sen4'

    assert create_ascii_str_from_str(string) == expected
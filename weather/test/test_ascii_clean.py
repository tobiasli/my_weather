from weather.utilities.ascii_clean import create_ascii_char_str_from_str

def test_ascii_clean():
    string = 'Eftasåsen'
    expected = 'eftas-sen'

    assert create_ascii_char_str_from_str(string) == expected
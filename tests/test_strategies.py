import unittest
from modules.strategies.strategy import JsonStrategy
from modules.strategies.strategy import GenericStrategy

class TestStrategy(unittest.TestCase):
    
    def test_parser_on_strategy_generic_(self):
        line = "MBL,4567822"
        sparator_column =";"
        generic = GenericStrategy("-")
        self.assertIsNotNone(generic.parser(line))


    def test_parser_on_strategy_json_(self):
        line = "{'MBL':'abn'}"
        sparator_column =";"
        json_st = JsonStrategy()
        self.assertIsNotNone(json_st.parser(line))    

if __name__ == '__main__':
    unittest.main()
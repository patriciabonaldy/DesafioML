from modules.strategies.genericStrategy import GenericStrategy
from modules.strategies.jsonStrategy import JsonStrategy
import json


class Strategy:

    
    def __init__(self, typeExtension, limitedColumn, limitedLine):
        self.typeExtension  = typeExtension
        self.limitedColumn  = limitedColumn
        self.limitedLine    = limitedLine

    
    def get_strategy(self):
        strategy = None
        if self.typeExtension == 'jsonl': 
            strategy = JsonStrategy()
        else:
            strategy = GenericStrategy(self.limitedColumn)
              
        return strategy

from decimal import Decimal
from CompilerStructuresModule.CompilerStructures.serializable import Serializable


class ResultTracker(Serializable):
    def __init__(self, fromDict=None):
        if fromDict is None:
            self.wins = 0
            self.losses = 0
            self.draws = 0
            self.potential_total = 0
        else:
            self.wins = fromDict['wins']
            self.losses = fromDict['losses']
            self.draws = fromDict['draws']
            self.potential_total = fromDict['potential_total']
    
    def __str__(self):
        return f"\nwins: {self.wins}, losses: {self.losses}, draws: {self.draws}, potential_total: {self.potential_total}"
    
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)
    
    def __incrementitem__(self, key):
        setattr(self, key, getattr(self, key) + 1)

    def get_winrate(self):
        return self.wins / self.potential_total if self.potential_total > 0 else 0

    def get_formatted_winrate(self):
        w = self.get_winrate()
        if w == 0:
            return "-"
        return str(round(w * 100))

    def get_outcome_rate(self):
        return (self.wins + self.losses + self.draws) / self.potential_total if self.potential_total > 0 else 0

    def get_formatted_outcome_rate(self):
        w = self.get_outcome_rate()
        if w == 0:
            return "-"
        return str(round(w * 100))

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
    
    def __incrementitem__(self, key):
        setattr(self, key, getattr(self, key) + 1)

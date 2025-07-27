from CompilerStructuresModule.CompilerStructures.serializable import Serializable

class ResultTracker(Serializable):
    def __init__(self):
        self.wins = 0
        self.losses = 0
        self.draws = 0
        self.potential_total = 0
    
    def __str__(self):
        return f"\nwins: {self.wins}, losses: {self.losses}, draws: {self.draws}, potential_total: {self.potential_total}"
    
    def __incrementitem__(self, key):
        setattr(self, key, getattr(self, key) + 1)

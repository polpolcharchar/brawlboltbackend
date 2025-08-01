from CompilerStructuresModule.CompilerStructures.serializable import Serializable


class FrequencyCompiler(Serializable):
    def __init__(self):
        self.frequencies = {}

    def add_entry(self, r):
        self.frequencies[str(r)] = self.frequencies.get(str(r), 0) + 1

    def get_average_entry(self):
        total = 0
        count = 0

        for rank_str, freq in self.frequencies.items():
            rank = float(rank_str)
            total += rank * freq
            count += freq

        return 0 if count == 0 else total / count
    
    def __str__(self):
        return str(self.frequencies)
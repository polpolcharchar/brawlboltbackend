from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.globalResultCompiler import GlobalResultCompiler


class PlayerResultCompiler(GlobalResultCompiler):
    duration_bucket_size = 30

    def __init__(self, fromDict=None):
        super().__init__(fromDict)

        if fromDict is None:
            self.duration_frequencies = FrequencyCompiler()
            self.player_trophy_change = 0
        else:
            self.duration_frequencies = FrequencyCompiler(fromDict['duration_frequencies'])
            self.player_trophy_change = fromDict['player_trophy_change']
    
    def handle_battle_result(self, match_data):
        super().handle_battle_result(match_data)

        self.player_trophy_change += match_data.trophy_change
        if match_data.duration:
            self.duration_frequencies.add_entry(match_data.duration // PlayerResultCompiler.duration_bucket_size)

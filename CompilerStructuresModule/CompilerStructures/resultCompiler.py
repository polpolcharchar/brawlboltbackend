from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.resultTracker import ResultTracker
from CompilerStructuresModule.CompilerStructures.serializable import Serializable

class ResultCompiler(Serializable):
    duration_bucket_size = 30

    def __init__(self):
        self.player_result_data = ResultTracker()
        self.player_star_data = ResultTracker()

        self.duration_frequencies = FrequencyCompiler()
        self.player_trophy_change = 0
    
    def handle_battle_result(self, match_data):
        self.player_result_data.__incrementitem__(match_data.result_type)
        self.player_result_data.potential_total += 1

        if match_data.star_player_exists:
            self.player_star_data.potential_total += 1

            if match_data.is_star_player:
                self.player_star_data.__incrementitem__(match_data.result_type)

        self.player_trophy_change += match_data.trophy_change
        if match_data.duration:
            self.duration_frequencies.add_entry(match_data.duration // ResultCompiler.duration_bucket_size)

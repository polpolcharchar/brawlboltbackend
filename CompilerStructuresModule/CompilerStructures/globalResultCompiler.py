from CompilerStructuresModule.CompilerStructures.serializable import Serializable
from CompilerStructuresModule.CompilerStructures.resultTracker import ResultTracker


class GlobalResultCompiler(Serializable):

    def __init__(self, fromDict=None):
        self.player_result_data = ResultTracker(fromDict['player_result_data'] if fromDict else None)
        self.player_star_data = ResultTracker(fromDict['player_star_data'] if fromDict else None)
    
    def __str__(self):
        return (
                f"\nplayer_result_data: {self.player_result_data}, "
                f"\nplayer_star_data: {self.player_star_data}, "
                )

    def handle_battle_result(self, match_data):

        self.player_result_data.__incrementitem__(match_data.result_type)
        self.player_result_data.potential_total += 1

        if match_data.star_player_exists:
            self.player_star_data.potential_total += 1

            if match_data.is_star_player:
                self.player_star_data.__incrementitem__(match_data.result_type)

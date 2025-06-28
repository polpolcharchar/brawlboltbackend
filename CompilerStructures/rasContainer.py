from CompilerStructures.recursiveAttributeStructure import RecursiveAttributeStructure
from CompilerStructures.serializable import Serializable


class RAS_Container(Serializable):
    def __init__(self, isGlobal, fromDict=None):

        if fromDict is None:

            #mode_brawler is always used
            self.mode_brawler = RecursiveAttributeStructure(isGlobal, ["mode", "brawler"])

            if isGlobal:
                #brawler is needed for global only
                self.brawler = RecursiveAttributeStructure(isGlobal, ["brawler"])
            else:
                #these are needed for player only
                self.brawler_mode_map = RecursiveAttributeStructure(isGlobal, ["brawler", "mode", "map"])
                self.mode_map_brawler = RecursiveAttributeStructure(isGlobal, ["mode", "map", "brawler"])
        else:
            #mode_brawler is always used
            self.mode_brawler = RecursiveAttributeStructure(isGlobal, ["mode", "brawler"], fromDict['mode_brawler'])

            if isGlobal:
                #brawler is needed for global only
                self.brawler = RecursiveAttributeStructure(isGlobal, ["brawler"], fromDict['brawler'])
            else:
                #these are needed for player only
                self.brawler_mode_map = RecursiveAttributeStructure(isGlobal, ["brawler", "mode", "map"], fromDict['brawler_mode_map'])
                self.mode_map_brawler = RecursiveAttributeStructure(isGlobal, ["mode", "map", "brawler"], fromDict['mode_map_brawler'])
    
    def handle_battle(self, match_data):
        self.mode_brawler.handle_battle_result(match_data)

        if hasattr(self, "brawler"):
            self.brawler.handle_battle_result(match_data)
        else:
            self.brawler_mode_map.handle_battle_result(match_data)
            self.mode_map_brawler.handle_battle_result(match_data)
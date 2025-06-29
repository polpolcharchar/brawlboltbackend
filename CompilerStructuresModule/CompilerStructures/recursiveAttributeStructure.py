from CompilerStructuresModule.CompilerStructures.globalResultCompiler import GlobalResultCompiler
from CompilerStructuresModule.CompilerStructures.playerResultCompiler import PlayerResultCompiler
from CompilerStructuresModule.CompilerStructures.serializable import Serializable


class RecursiveAttributeStructure(Serializable):
    def __init__(self, isGlobal, stat_chains, fromDict=None):
        if fromDict is None:
            if isGlobal:
                self.overall = GlobalResultCompiler()
            else:
                self.overall = PlayerResultCompiler()

            self.isGlobal = isGlobal
                
            self.stat_chain = stat_chains

            if self.stat_chain:
                # setattr(self, self.stat_chain[0], {})
                setattr(self, 'stat_map', {})
        else:
            if not isinstance(fromDict, dict):
                fromDict = fromDict.to_dict()

            if isGlobal:
                self.overall = GlobalResultCompiler(fromDict['overall'])
            else:
                self.overall = PlayerResultCompiler(fromDict['overall'])
            
            self.isGlobal = isGlobal

            self.stat_chain = fromDict['stat_chain']


            if self.stat_chain:
                oldMapName = 'stat_map'
                if 'stat_map' not in fromDict:
                    oldMapName = self.stat_chain[0]

                nextStatMap = {key: RecursiveAttributeStructure(isGlobal, value['stat_chain'], value) for key, value in fromDict[oldMapName].items()}
                setattr(self, 'stat_map', nextStatMap)
    
    def exclude_attributes(self):
        return ["isGlobal"]

    def __str__(self):
        stat_map_str = ", ".join(f"{key}: {value}" for key, value in self.get_next_stat_map().items())
        return f"\nstat_chain: {self.stat_chain}, " \
               f"\noverall: {self.overall}, " \
               f"\nstat_map: {{{stat_map_str}}}"

    def get_next_stat_map(self):
        if not self.stat_chain:
            return {}

        # return getattr(self, self.stat_chain[0])
        return getattr(self, 'stat_map')

    def handle_battle_result(self, match_data):
        self.overall.handle_battle_result(match_data=match_data)

        if self.stat_chain:
            stat_map = self.get_next_stat_map()
            # stat_value = match_data[self.stat_chain[0]]
            stat_value = match_data.__getitem__(self.stat_chain[0])

            if stat_value not in stat_map:
                stat_map[stat_value] = RecursiveAttributeStructure(self.isGlobal, self.stat_chain[1:])

            stat_map[stat_value].handle_battle_result(match_data=match_data)

import math
from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.matchData import MatchData
from CompilerStructuresModule.CompilerStructures.gameAttributeTrie import GameAttributeTrie
from CompilerStructuresModule.CompilerStructures.serializable import Serializable
from DatabaseUtility.modeToMapOverrideUtility import getMode


class BrawlStats(Serializable):
    def __init__(self, isGlobal, playerDataJSON=None):

        self.isGlobal = isGlobal

        self.typeModeBrawler = GameAttributeTrie(isGlobal, ["type", "mode", "brawler"])
        self.typeModeBrawler.populateNextStructure("regular", playerDataJSON["regular_mode_brawler"] if playerDataJSON else None)
        self.typeModeBrawler.populateNextStructure("ranked", playerDataJSON["ranked_mode_brawler"] if playerDataJSON else None)

        if self.isGlobal:
            self.typeBrawler = GameAttributeTrie(isGlobal, ["type", "brawler"])
            self.typeBrawler.populateNextStructure("regular", playerDataJSON["regular_brawler"] if playerDataJSON else None)
            self.typeBrawler.populateNextStructure("ranked", playerDataJSON["ranked_brawler"] if playerDataJSON else None)
        else:
            self.typeBrawlerModeMap = GameAttributeTrie(isGlobal, ["type", "brawler", "mode", "map"])
            self.typeBrawlerModeMap.populateNextStructure("regular", playerDataJSON["regular_brawler_mode_map"] if playerDataJSON else None)
            self.typeBrawlerModeMap.populateNextStructure("ranked", playerDataJSON["ranked_brawler_mode_map"] if playerDataJSON else None)

            self.typeModeMapBrawler = GameAttributeTrie(isGlobal, ["type", "mode", "map", "brawler"])
            self.typeModeMapBrawler.populateNextStructure("regular", playerDataJSON["regular_mode_map_brawler"] if playerDataJSON else None)
            self.typeModeMapBrawler.populateNextStructure("ranked", playerDataJSON["ranked_mode_map_brawler"] if playerDataJSON else None)

        if playerDataJSON is None or isGlobal:
            self.showdown_rank_compilers = {}
        else:
            self.showdown_rank_compilers = {key: FrequencyCompiler(value) for key, value in playerDataJSON['showdown_rank_compilers'].items()}

        # Put all in a list for easy compiling
        self.gameAttributeTries = []
        self.gameAttributeTries.append(self.typeModeBrawler)
        if self.isGlobal:
            self.gameAttributeTries.append(self.typeBrawler)
        else:
            self.gameAttributeTries.append(self.typeBrawlerModeMap)
            self.gameAttributeTries.append(self.typeModeMapBrawler)

    def exclude_attributes(self):
        return ["isGlobal", "gameAttributeTries"]

    def handleBattles(self, battles, player_tag=""):
        if not self.isGlobal and player_tag == "":
            print("Player-specific provided no player tag!")
            return False

        for battle in battles:
            if self.isGlobal:
                player_tag = battle['player_tag']
            
            if 'rank' in battle['battle']:
                self.handleShowdownBattle(battle, player_tag)
            elif getMode(battle) == "duels":
                self.handleDuelsBattle(battle, player_tag)
            elif not 'teams' in battle['battle']:
                continue
            elif len(battle['battle']['teams']) != 2:
                continue
            else:
                self.handleStandardBattle(battle, player_tag)
    
    def handleShowdownBattle(self, battle, player_tag):

        def getShowdownTeamPlayers(battle, player_tag):
            if 'players' in battle['battle']:
                for player in battle['battle']['players']:
                    if player['tag'] == player_tag:
                        return [player]
            elif 'teams' in battle['battle']:
                # Find the team that this player is on
                team_index = -1
                for i in range(len(battle['battle']['teams'])):
                    for j in range(len(battle['battle']['teams'][i])):
                        if battle['battle']['teams'][i][j]['tag'] == player_tag:
                            team_index = i

                if team_index == -1:
                    print("Unable to find team!")
                    return

                return battle['battle']['teams'][team_index]
            else:
                print("Showdown has no players or teams!")
                return []

        # Update rank compilers:
        if getMode(battle) not in self.showdown_rank_compilers:
            self.showdown_rank_compilers[getMode(battle)] = FrequencyCompiler()
        self.showdown_rank_compilers[getMode(battle)].add_entry(battle['battle']['rank'])

        # Update normal compilers:

        # Collect variables:
        is_star_player = battle['battle']['rank'] == 1
        result_type = "wins" if self.isShowdownVictory(battle) else "losses"

        playersOnThisTeam = getShowdownTeamPlayers(battle, player_tag)
        for player in playersOnThisTeam:

            if self.isGlobal:
                for gameAttributeTrie in self.gameAttributeTries:
                    gameAttributeTrie.handle_battle_result(
                        MatchData(
                            battle['event']['map'],
                            getMode(battle),
                            player['brawler']['name'],
                            result_type, is_star_player,
                            True,
                            None,
                            None,
                            self.getType(battle)
                        )
                    )
            else:
                if player['tag'] != player_tag:
                    continue

                for gameAttributeTrie in self.gameAttributeTries:
                    gameAttributeTrie.handle_battle_result(
                        MatchData(
                            battle['event']['map'],
                            getMode(battle),
                            player['brawler']['name'],
                            result_type,
                            is_star_player,
                            True,
                            None,
                            self.getTrophyChange(battle, player_tag),
                            self.getType(battle)
                        )
                    )
    
    def handleDuelsBattle(self, battle, player_tag):
        if not 'players' in battle['battle']:
            print("Duel has no players!")
            return
        
        for player in battle['battle']['players']:
            result_type = (
                "draws"
                if battle['battle']['result'] == "draw"
                else ("wins" if player_tag == player['tag'] and battle['battle']['result'] == "victory" or player_tag != player['tag'] and battle['battle']['result'] == "defeat" else "losses")
            )

            if self.isGlobal:
                for brawler in player['brawlers']:
                    for gameAttributeTrie in self.gameAttributeTries:
                        gameAttributeTrie.handle_battle_result(
                            MatchData(
                                battle['event']['map'],
                                getMode(battle),
                                brawler['name'],
                                result_type,
                                False,
                                False,
                                None,
                                None,
                                self.getType(battle)
                            )
                        )
            else:
                if player['tag'] != player_tag:
                    continue

                for brawler in player['brawlers']:
                    for gameAttributeTrie in self.gameAttributeTries:
                        gameAttributeTrie.handle_battle_result(
                            MatchData(
                                battle['event']['map'],
                                getMode(battle),
                                brawler['name'],
                                result_type,
                                False,
                                False,
                                battle['battle']['duration'],
                                self.getTrophyChange(battle, player_tag),
                                self.getType(battle)
                            )
                        )

    def handleStandardBattle(self, battle, player_tag):
        
        winningTeamIndex = self.getWinningTeamIndex(battle, player_tag)

        for teamIndex in range(2):
            for player in battle['battle']['teams'][teamIndex]:

                is_star_player = player['tag'] == battle['battle']['starPlayer']['tag'] if battle['battle']['starPlayer'] else False
                result_type = (
                    "draws"
                    if battle['battle']['result'] == "draw"
                    else ("wins" if winningTeamIndex == teamIndex else "losses")
                )

                #If this is player statistics, only include the player
                if not self.isGlobal and player['tag'] != player_tag:
                    continue

                for gameAttributeTrie in self.gameAttributeTries:
                    gameAttributeTrie.handle_battle_result(
                        MatchData(
                            battle['event']['map'],
                            getMode(battle),
                            player['brawler']['name'],
                            result_type,
                            is_star_player,
                            battle['battle']['starPlayer'] is not None,
                            battle['battle']['duration'],
                            self.getTrophyChange(battle, player_tag),
                            self.getType(battle)
                        )
                    )

    def getGameAttributeTrie(self, statPath):
        if statPath == "regularModeBrawler":
            return self.typeModeBrawler.get_next_stat_map()["regular"]
        elif statPath == "regularBrawler":
            return self.typeBrawler.get_next_stat_map()["regular"]
        elif statPath == "regularBrawlerModeMap":
            return self.typeBrawlerModeMap.get_next_stat_map()["regular"]
        elif statPath == "regularModeMapBrawler":
            return self.typeModeMapBrawler.get_next_stat_map()["regular"]

        elif statPath == "rankedModeBrawler":
            return self.typeModeBrawler.get_next_stat_map()["ranked"]
        elif statPath == "rankedBrawler":
            return self.typeBrawler.get_next_stat_map()["ranked"]
        elif statPath == "rankedBrawlerModeMap":
            return self.typeBrawlerModeMap.get_next_stat_map()["ranked"]
        elif statPath == "rankedModeMapBrawler":
            return self.typeModeMapBrawler.get_next_stat_map()["ranked"]

        else:
            raise KeyError("Invalid Stat Path!")

    # Unfinished!
    # Need to update to new format!
    def merge(self, otherBrawlStats):

        if self.isGlobal != otherBrawlStats.isGlobal:
            print("Cannot merge two BrawlStats of different types!")
            return False

        self.regular_stat_compilers.merge(otherBrawlStats.regular_stat_compilers)
        self.ranked_stat_compilers.merge(otherBrawlStats.ranked_stat_compilers)
        
        seenShowdownTypes = set()
        for showdownType in self.showdown_rank_compilers:
            self.showdown_rank_compilers[showdownType].merge(otherBrawlStats.showdown_rank_compilers[showdownType])
            seenShowdownTypes.add(showdownType)
        
        for otherShowdownType in otherBrawlStats.showdown_rank_compilers:
            if otherShowdownType not in seenShowdownTypes:
                self.showdown_rank_compilers[otherShowdownType] = otherBrawlStats.showdown_rank_compilers[otherShowdownType]

    # Utility Functions
    def getWinningTeamIndex(self, game, playerTag):
        result = 1 if (game['battle']['result'] == "victory") else 0
        for player in game['battle']['teams'][0]:
            if player['tag'] == playerTag:
                return 1 - result
        return result
    def getTrophyChange(self, game, playerTag):
        if 'trophyChange' in game['battle']:
            return game['battle']['trophyChange']
        
        if 'players' in game['battle'] and 'brawlers' in game['battle']['players'][0]:
            for player in game['battle']['players']:
                if player['tag'] == playerTag:
                    total = 0
                    for brawler in player['brawlers']:
                        total += brawler['trophyChange']
                    return total
        
        return 0#change this!!
    def isShowdownVictory(self, game):
        if not 'rank' in game['battle']:
            raise KeyError("This isn't a showdown game!")
        
        if 'players' in game['battle']:
            return game['battle']['rank'] <= math.floor(len(game['battle']['players']) / 2)
        else:
            return game['battle']['rank'] <= math.floor(len(game['battle']['teams']) / 2)
    def getType(self, battle):
        return "ranked" if battle['battle']['type'] == "soloRanked" else "regular"
    
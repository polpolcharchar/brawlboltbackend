import math
import boto3
import json
from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.matchData import MatchData
from CompilerStructuresModule.CompilerStructures.rasContainer import RAS_Container
from CompilerStructuresModule.CompilerStructures.serializable import Serializable


class BrawlStats(Serializable):
    def __init__(self, isGlobal, fromDict=None):

        self.mapToModeOverrides = None
        self.isGlobal = isGlobal

        if fromDict is None:
            self.regular_stat_compilers = RAS_Container(isGlobal)
            self.ranked_stat_compilers = RAS_Container(isGlobal)
            self.showdown_rank_compilers = {}
        else:
            self.regular_stat_compilers = RAS_Container(isGlobal, fromDict['regular_stat_compilers'])
            self.ranked_stat_compilers = RAS_Container(isGlobal, fromDict['ranked_stat_compilers'])
            self.showdown_rank_compilers = {key: FrequencyCompiler(value) for key, value in fromDict['showdown_rank_compilers'].items()}
    
    def exclude_attributes(self):
        return ["isGlobal", "mapToModeOverrides"]

    def handleBattles(self, battles, player_tag=""):
        if not self.isGlobal and player_tag == "":
            print("Player-specific provided no palyer tag!")
            return False

        for battle in battles:
            if self.isGlobal:
                player_tag = battle['player_tag']
            
            if 'rank' in battle['battle']:
                self.handleShowdownBattle(battle, player_tag)
            elif self.getMode(battle) == "duels":
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
        if self.getMode(battle) not in self.showdown_rank_compilers:
            self.showdown_rank_compilers[self.getMode(battle)] = FrequencyCompiler()
        self.showdown_rank_compilers[self.getMode(battle)].add_entry(battle['battle']['rank'])

        # Update normal compilers:

        # Collect variables:
        is_star_player = battle['battle']['rank'] == 1
        result_type = "wins" if self.isShowdownVictory(battle) else "losses"

        playersOnThisTeam = getShowdownTeamPlayers(battle, player_tag)
        for player in playersOnThisTeam:

            if self.isGlobal:
                self.regular_stat_compilers.handle_battle(
                    MatchData(
                        battle['event']['map'],
                        self.getMode(battle),
                        player['brawler']['name'],
                        result_type, is_star_player,
                        True,
                        None,
                        None
                    )
                )
            else:
                if player['tag'] != player_tag:
                    continue

                self.regular_stat_compilers.handle_battle(
                    MatchData(
                        battle['event']['map'],
                        self.getMode(battle),
                        player['brawler']['name'],
                        result_type,
                        is_star_player,
                        True,
                        None,
                        self.getTrophyChange(battle, player_tag)
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
                    self.regular_stat_compilers.handle_battle(
                        MatchData(
                            battle['event']['map'],
                            self.getMode(battle),
                            brawler['name'],
                            result_type,
                            False,
                            False,
                            None,
                            None
                        )
                    )
            else:
                if player['tag'] != player_tag:
                    continue

                for brawler in player['brawlers']:
                    self.regular_stat_compilers.handle_battle(
                        MatchData(
                            battle['event']['map'],
                            self.getMode(battle),
                            brawler['name'],
                            result_type,
                            False,
                            False,
                            battle['battle']['duration'],
                            self.getTrophyChange(battle, player_tag)
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

                compilers_to_update = self.ranked_stat_compilers if battle['battle']['type'] == "soloRanked" else self.regular_stat_compilers

                #If this is player statistics, only include the player
                if not self.isGlobal and player['tag'] != player_tag:
                    continue

                compilers_to_update.handle_battle(
                    MatchData(
                        battle['event']['map'],
                        self.getMode(battle),
                        player['brawler']['name'],
                        result_type,
                        is_star_player,
                        battle['battle']['starPlayer'] is not None,
                        battle['battle']['duration'],
                        self.getTrophyChange(battle, player_tag)
                    )
                )

    # Unfinished!
    def merge(self, otherBrawlStats):

        if self.isGlobal != otherBrawlStats.isGlobal:
            print("Cannot merge two BrawlStats of different types!")
            return False

        if self.mapToModeOverrides is None and otherBrawlStats.mapToModeOverrides is not None:
            self.mapToModeOverrides = otherBrawlStats.mapToModeOverrides

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

    def getMode(self, game):
        def fetchAndAssignOverrides():
            DYNAMODB_REGION = 'us-west-1'
            GLOBAL_STATS_TABLE = "BrawlStarsMapToModeOverrides"
            dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

            try:
                response = dynamodb.get_item(
                    TableName=GLOBAL_STATS_TABLE,
                    Key={'overrideType': {'S': 'standard'}}
                )

                if 'Item' in response and 'overrides' in response['Item']:
                    overrides_raw = response['Item']['overrides']['S']
                    overrides = json.loads(overrides_raw)
                    self.mapToModeOverrides = overrides
                else:
                    print("Overrides not found.")
                    self.mapToModeOverrides = {}
            except Exception as e:
                print(f"An error occurred: {e}")
                self.mapToModeOverrides = {}

        if self.mapToModeOverrides is None:
            print("Fetching")
            fetchAndAssignOverrides()

        if 'map' in game['event'] and game['event']['map'] in self.mapToModeOverrides:
            return self.mapToModeOverrides[game['event']['map']]
        elif 'mode' in game['event'] and game['event']['mode'] != "unknown":
            return game['event']['mode']
        elif 'mode' in game['battle']:
            return game['battle']['mode']
        else:
            return "unknown"
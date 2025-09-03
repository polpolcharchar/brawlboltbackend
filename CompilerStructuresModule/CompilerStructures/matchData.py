import math
from CompilerStructuresModule.CompilerStructures.serializable import Serializable
from DatabaseUtility.modeToMapOverrideUtility import getMode

class MatchData(Serializable):
    def __init__(self, map, mode, brawler, result_type, is_star_player, star_player_exists, duration, trophy_change, type):
        if map == None:
            self.map = "unknown"
        else:
            self.map = map

        self.mode = mode
        self.brawler = brawler
        self.result_type = result_type
        self.is_star_player = is_star_player
        self.star_player_exists = star_player_exists
        self.duration = duration
        self.trophy_change = trophy_change
        self.type = type

    def __getitem__(self, key):
        return getattr(self, key)

def getMatchDataObjectsFromGame(game, playerTag, includeAllPlayers):
    def getTypes(game, checkForLegendary=False):
        typesResult = ["ranked"] if game['battle']['type'] == "soloRanked" else ["regular"]

        # check if any player is legendary 1 or higher
        if checkForLegendary and game['battle']['type'] == 'soloRanked':
            for teamIndex in range(2):
                for player in game['battle']['teams'][teamIndex]:
                    if 'brawler' in player:
                        # For some reason, some players in ranked show their actual brawler trophies, instead of the rank value
                        # Limit this to specifically between legendary 1 and pro so that real trophy counts don't trigger this
                        if player['brawler']['trophies'] >= 16 and player['brawler']['trophies'] <= 22:
                            typesResult.append("legendaryOrHigher")
                            return typesResult
        
        return typesResult
    def isShowdownVictory(game):
        if not 'rank' in game['battle']:
            raise KeyError("This isn't a showdown game!")
        
        if 'players' in game['battle']:
            return game['battle']['rank'] <= math.floor(len(game['battle']['players']) / 2)
        else:
            return game['battle']['rank'] <= math.floor(len(game['battle']['teams']) / 2)
    def getTrophyChange(game, playerTag):
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
    def getWinningTeamIndex(game, playerTag):
        result = 1 if (game['battle']['result'] == "victory") else 0
        for player in game['battle']['teams'][0]:
            if player['tag'] == playerTag:
                return 1 - result
        return result

    def getMatchDataFromShowdown(game, playerTag):
        def getShowdownTeamPlayers(game, playerTag):
            if 'players' in game['battle']:
                for player in game['battle']['players']:
                    if player['tag'] == playerTag:
                        return [player]
            elif 'teams' in game['battle']:
                # Find the team that this player is on
                team_index = -1
                for i in range(len(game['battle']['teams'])):
                    for j in range(len(game['battle']['teams'][i])):
                        if game['battle']['teams'][i][j]['tag'] == playerTag:
                            team_index = i

                if team_index == -1:
                    print("Unable to find team!")
                    return []

                return game['battle']['teams'][team_index]
            else:
                print("Showdown has no players or teams!")
                return []

        # Collect Variables:
        is_star_player = game['battle']['rank'] == 1
        result_type = "wins" if isShowdownVictory(game) else "losses"

        playersOnThisTeam = getShowdownTeamPlayers(game, playerTag)

        result = []
        for player in playersOnThisTeam:
            if includeAllPlayers or player['tag'] == playerTag:
                types = getTypes(game)
                for type in types:
                    result.append(MatchData(
                        game['event']['map'],
                        getMode(game),
                        player['brawler']['name'],
                        result_type,
                        is_star_player,
                        True,
                        None,
                        getTrophyChange(game, player['tag']),
                        type
                    ))
        
        return result

    def getMatchDataFromDuels(game, playerTag):
        result = []

        for player in game['battle']['players']:
            result_type = (
                "draws"
                if game['battle']['result'] == "draw"
                else ("wins" if playerTag == player['tag'] and game['battle']['result'] == "victory" or playerTag != player['tag'] and game['battle']['result'] == "defeat" else "losses")
            )

            if includeAllPlayers or player['tag'] == playerTag:
                for brawler in player['brawlers']:
                    types = getTypes(game)
                    for type in types:
                        result.append(MatchData(
                                game['event']['map'],
                                getMode(game),
                                brawler['name'],
                                result_type,
                                False,
                                False,
                                game['battle']['duration'],
                                getTrophyChange(game, player['tag']),
                                type
                            )
                        )
            
        return result

    def getMatchDataFromRegular(game, playerTag):
        result = []

        winningTeamIndex = getWinningTeamIndex(game, playerTag)

        for teamIndex in range(2):
            for player in game['battle']['teams'][teamIndex]:

                is_star_player = player['tag'] == game['battle']['starPlayer']['tag'] if game['battle']['starPlayer'] else False
                result_type = (
                    "draws"
                    if game['battle']['result'] == "draw"
                    else ("wins" if winningTeamIndex == teamIndex else "losses")
                )

                if includeAllPlayers or player['tag'] == playerTag:
                    types = getTypes(game, checkForLegendary=True)
                    for type in types:
                        result.append(MatchData(
                                game['event']['map'],
                                getMode(game),
                                player['brawler']['name'],
                                result_type,
                                is_star_player,
                                game['battle']['starPlayer'] is not None,
                                game['battle']['duration'],
                                getTrophyChange(game, player['tag']),
                                type
                            )
                        )

        return result

    if 'rank' in game['battle']:
        return getMatchDataFromShowdown(game, playerTag)
    elif getMode(game) == "duels":
        return getMatchDataFromDuels(game, playerTag)
    elif 'teams' not in game['battle']:
        return []
    elif len(game['battle']['teams']) != 2:
        return []
    else:
        return getMatchDataFromRegular(game, playerTag)

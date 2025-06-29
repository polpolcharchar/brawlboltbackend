import math
from decimal import Decimal
import json
import boto3

mapToModeOverrides = None
def getMode(game):
    def fetchAndAssignOverrides():
        global mapToModeOverrides
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
                mapToModeOverrides = overrides
            else:
                print("Overrides not found.")
                mapToModeOverrides = {}
        except Exception as e:
            print(f"An error occurred: {e}")
            mapToModeOverrides = {}

    if mapToModeOverrides is None:
        print("Fetching")
        fetchAndAssignOverrides()

    if 'map' in game['event'] and game['event']['map'] in mapToModeOverrides:
        return mapToModeOverrides[game['event']['map']]
    elif 'mode' in game['event'] and game['event']['mode'] != "unknown":
        return game['event']['mode']
    elif 'mode' in game['battle']:
        return game['battle']['mode']
    else:
        return "unknown"

def getWinningTeamIndex(game, playerTag):
    result = 1 if (game['battle']['result'] == "victory") else 0
    for player in game['battle']['teams'][0]:
        if player['tag'] == playerTag:
            return 1 - result
    return result

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

def isShowdownVictory(game):
    if not 'rank' in game['battle']:
        raise KeyError("This isn't a showdown game!")
    
    if 'players' in game['battle']:
        return game['battle']['rank'] <= math.floor(len(game['battle']['players']) / 2)
    else:
        return game['battle']['rank'] <= math.floor(len(game['battle']['teams']) / 2)

from typing import get_type_hints, Type, Any

class Serializable:
    def to_dict(self):
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, Serializable):
                result[k] = v.to_dict()
            elif isinstance(v, dict):
                # recursively handle dict values
                result[k] = {key: val.to_dict() if isinstance(val, Serializable) else val for key, val in v.items()}
            elif isinstance(v, list):
                # recursively handle lists
                result[k] = [item.to_dict() if isinstance(item, Serializable) else item for item in v]
            elif isinstance(v, Decimal):
                result[k] = int(v) if v % 1 == 0 else float(v)
            else:
                result[k] = v
        return result

    @classmethod
    def from_dict(cls: Type['Serializable'], data: dict) -> 'Serializable':
        obj = cls.__new__(cls)
        hints = get_type_hints(cls)
        for k, v in data.items():
            expected_type = hints.get(k, None)
            if isinstance(v, dict) and expected_type and issubclass_safe(expected_type, Serializable):
                setattr(obj, k, expected_type.from_dict(v))
            elif isinstance(v, dict):
                # For dicts, check if dict values need deserialization (if Serializable)
                new_dict = {}
                for key, val in v.items():
                    # Try to detect nested Serializable objects if possible (best effort)
                    if isinstance(val, dict):
                        # No way to know exact type, just set dict for now
                        new_dict[key] = val
                    else:
                        new_dict[key] = val
                setattr(obj, k, new_dict)
            elif isinstance(v, list):
                # If expected type hint for this attr is List[SomeSerializable], attempt deserialization
                if expected_type and hasattr(expected_type, '__args__'):
                    inner_type = expected_type.__args__[0]
                    if issubclass_safe(inner_type, Serializable):
                        setattr(obj, k, [inner_type.from_dict(item) if isinstance(item, dict) else item for item in v])
                    else:
                        setattr(obj, k, v)
                else:
                    setattr(obj, k, v)
            else:
                setattr(obj, k, v)
        return obj

def issubclass_safe(cls: Any, class_or_tuple) -> bool:
    try:
        return issubclass(cls, class_or_tuple)
    except TypeError:
        return False


class PlayerStats(Serializable):
    # def __init__(self, 
    #              regular_stat_compilers=None, 
    #              ranked_stat_compilers=None, 
    #              showdown_rank_compilers=None):
    #     """
    #     Constructor for the PlayerStats class.
        
    #     Arguments:
    #     - regular_stat_compilers: A TripleRecursiveStatCompiler instance or default to a new one.
    #     - ranked_stat_compilers: A TripleRecursiveStatCompiler instance or default to a new one.
    #     - showdown_rank_compilers: A dictionary or default to an empty dictionary.
    #     """
    #     # self.regular_stat_compilers = (
    #     #     regular_stat_compilers 
    #     #     if regular_stat_compilers is not None 
    #     #     else TripleRecursiveStatCompiler()
    #     # )
    #     # self.ranked_stat_compilers = (
    #     #     ranked_stat_compilers 
    #     #     if ranked_stat_compilers is not None 
    #     #     else TripleRecursiveStatCompiler()
    #     # )
    #     # self.showdown_rank_compilers = (
    #     #     showdown_rank_compilers 
    #     #     if showdown_rank_compilers is not None 
    #     #     else {}
    #     # )
    #     if regular_stat_compilers is not None:
    #         self.regular_stat_compilers = TripleRecursiveStatCompiler(
    #             regular_stat_compilers["brawler_mode_map"],
    #             regular_stat_compilers["mode_map_brawler"],
    #             regular_stat_compilers["mode_brawler"]
    #         )
    #     else:
    #         self.regular_stat_compilers = TripleRecursiveStatCompiler()
        
    #     if ranked_stat_compilers is not None:
    #         self.ranked_stat_compilers = TripleRecursiveStatCompiler(
    #             ranked_stat_compilers["brawler_mode_map"],
    #             ranked_stat_compilers["mode_map_brawler"],
    #             ranked_stat_compilers["mode_brawler"]
    #         )
    #     else:
    #         self.ranked_stat_compilers = TripleRecursiveStatCompiler()

    #     if showdown_rank_compilers is not None:
    #         self.showdown_rank_compilers = showdown_rank_compilers
    #     else:
    #         self.showdown_rank_compilers = {}



    # def __init__(self):
    #     self.regular_stat_compilers = TripleRecursiveStatCompiler()
    #     self.ranked_stat_compilers = TripleRecursiveStatCompiler()
    #     self.showdown_rank_compilers = {}


    def __init__(self, 
                 regular_stat_compilers=None, 
                 ranked_stat_compilers=None, 
                 showdown_rank_compilers=None):
        """
        Constructor for the PlayerStats class.
        
        Arguments:
        - regular_stat_compilers: A TripleRecursiveStatCompiler instance or default to a new one.
        - ranked_stat_compilers: A TripleRecursiveStatCompiler instance or default to a new one.
        - showdown_rank_compilers: A dictionary or default to an empty dictionary.
        """
        # self.regular_stat_compilers = (
        #     regular_stat_compilers 
        #     if regular_stat_compilers is not None 
        #     else TripleRecursiveStatCompiler()
        # )
        # self.ranked_stat_compilers = (
        #     ranked_stat_compilers 
        #     if ranked_stat_compilers is not None 
        #     else TripleRecursiveStatCompiler()
        # )
        # self.showdown_rank_compilers = (
        #     showdown_rank_compilers 
        #     if showdown_rank_compilers is not None 
        #     else {}
        # )
        if regular_stat_compilers is not None:
            self.regular_stat_compilers = TripleRecursiveStatCompiler(
                regular_stat_compilers["brawler_mode_map"],
                regular_stat_compilers["mode_map_brawler"],
                regular_stat_compilers["mode_brawler"]
            )
        else:
            self.regular_stat_compilers = TripleRecursiveStatCompiler()
        
        if ranked_stat_compilers is not None:
            self.ranked_stat_compilers = TripleRecursiveStatCompiler(
                ranked_stat_compilers["brawler_mode_map"],
                ranked_stat_compilers["mode_map_brawler"],
                ranked_stat_compilers["mode_brawler"]
            )
        else:
            self.ranked_stat_compilers = TripleRecursiveStatCompiler()

        if showdown_rank_compilers is not None:
            self.showdown_rank_compilers = showdown_rank_compilers
        else:
            self.showdown_rank_compilers = {}
        
        
    def handleBattles(self, battles):
        for battle in battles:

            # print("BATTLE NEW!")

            player_tag = battle['player_tag']

            # Showdown:
            if 'rank' in battle['battle']:

                # Update rank compilers:
                if getMode(battle) not in self.showdown_rank_compilers:
                    self.showdown_rank_compilers[getMode(battle)] = FrequencyCompiler()

                self.showdown_rank_compilers[getMode(battle)].add_entry(battle['battle']['rank'])

                is_star_player = battle['battle']['rank'] == 1
                result_type = "wins" if isShowdownVictory(battle) else "losses"

                # Will be assigned next:
                players = []

                if 'players' in battle['battle']:
                    for player in battle['battle']['players']:
                        if player['tag'] == player_tag:
                            players = [player]
                            break
                elif 'teams' in battle['battle']:
                    # Find the team that this player is on
                    team_index = -1
                    for i in range(len(battle['battle']['teams'])):
                        for j in range(len(battle['battle']['teams'][i])):
                            if battle['battle']['teams'][i][j]['tag'] == player_tag:
                                team_index = i

                    if team_index == -1:

                        print(battle)

                        # raise ValueError("Unable to find team!")
                        print("Unable to find team!")
                        continue

                    players = battle['battle']['teams'][team_index]
                else:
                    # raise ValueError("Showdown has no players or teams!")
                    print("Showdown has no players or teams!")
                    continue

                for player in players:

                    #ONLY INCLUDE PLAYER!
                    # if(player['tag'] != player_tag):
                    #     continue

                    # is_target_player = player['tag'] == player_tag

                    self.regular_stat_compilers.handle_battle(
                        MatchPlayerInfo(result_type, is_star_player, True),
                        MatchData(battle['event']['map'], getMode(battle), player['brawler']['name'])
                    )

                continue

            if getMode(battle) == "duels":

                if not 'players' in battle['battle']:
                    # raise ValueError("Duel has no players!")
                    print("Duel has no players!")
                    continue

                for player in battle['battle']['players']:

                    #ONLY INCLUDE PLAYER
                    # if player['tag'] != player_tag:
                    #     continue

                    result_type = (
                        "draws"
                        if battle['battle']['result'] == "draw"
                        else ("wins" if player_tag == player['tag'] and battle['battle']['result'] == "victory" or player_tag != player['tag'] and battle['battle']['result'] == "defeat" else "losses")
                    )

                    for brawler in player['brawlers']:
                        self.regular_stat_compilers.handle_battle(
                            MatchPlayerInfo(result_type, False, False),
                            MatchData(battle['event']['map'], getMode(battle), brawler['name'])
                        )

                continue

            # Check conditions:
            if not 'teams' in battle['battle']:
                # raise ValueError("Versus has no teams!")
                print("Versus has no teams!")
                continue

            if len(battle['battle']['teams']) != 2:
                # raise ValueError("Not 2 teams!")
                # print("Not 2 teams!")
                continue

            # Normal versus modes:
            winning_index = getWinningTeamIndex(battle, player_tag)

            for team_index in range(2):
                for player in battle['battle']['teams'][team_index]:

                    #ONLY INCLUDE PLAYER!
                    # if(player['tag'] != player_tag):
                    #     continue

                    is_star_player = player['tag'] == battle['battle']['starPlayer']['tag'] if battle['battle']['starPlayer'] else False
                    result_type = (
                        "draws"
                        if battle['battle']['result'] == "draw"
                        else ("wins" if winning_index == team_index else "losses")
                    )

                    compilers_to_update = self.ranked_stat_compilers if battle['battle']['type'] == "soloRanked" else self.regular_stat_compilers

                    mpi = MatchPlayerInfo(result_type, is_star_player, battle['battle']['starPlayer'] is not None)
                    md = MatchData(battle['event']['map'], getMode(battle), player['brawler']['name'])

                    # print("Handle battle: " + str(mpi.result_type) + "    " + str(md.brawler))
                    compilers_to_update.handle_battle(
                        mpi,
                        md
                    )

    def __str__(self):
        return (f"\nregular_stat_compilers: {self.regular_stat_compilers}, "
                f"\nranked_stat_compilers: {self.ranked_stat_compilers}, "
                f"\nshowdown_rank_compilers: {self.showdown_rank_compilers}")

class TripleRecursiveStatCompiler(Serializable):
    # def __init__(self,
    #              brawler_mode_map = None,
    #              mode_map_brawler = None,
    #              mode_brawler = None):
        
    #     if brawler_mode_map is not None:
    #         self.brawler_mode_map = brawler_mode_map
    #     else:
    #         self.brawler_mode_map = RecursiveStatCompiler(["brawler", "mode", "map"])

    #     if mode_map_brawler is not None:
    #         self.mode_map_brawler = mode_map_brawler
    #     else:
    #         self.mode_map_brawler = RecursiveStatCompiler(["mode", "map", "brawler"])

    #     if mode_brawler is not None:
    #         self.mode_brawler = mode_brawler
    #     else:
    #         self.mode_brawler = RecursiveStatCompiler(["mode", "brawler"])



    def __init__(self):
        self.brawler_mode_map = RecursiveStatCompiler(["brawler", "mode", "map"])
        self.mode_map_brawler = RecursiveStatCompiler(["mode", "map", "brawler"])
        self.mode_brawler = RecursiveStatCompiler(["mode", "brawler"])
        self.brawler = RecursiveStatCompiler(["brawler"])



    # def __init__(self,
    #              brawler_mode_map = None,
    #              mode_map_brawler = None,
    #              mode_brawler = None):
        
    #     if brawler_mode_map is not None:
    #         self.brawler_mode_map = brawler_mode_map
    #     else:
    #         self.brawler_mode_map = RecursiveStatCompiler(["brawler", "mode", "map"])

    #     if mode_map_brawler is not None:
    #         self.mode_map_brawler = mode_map_brawler
    #     else:
    #         self.mode_map_brawler = RecursiveStatCompiler(["mode", "map", "brawler"])

    #     if mode_brawler is not None:
    #         self.mode_brawler = mode_brawler
    #     else:
    #         self.mode_brawler = RecursiveStatCompiler(["mode", "brawler"])
    
    def __str__(self):
        return (f"\nbrawler_mode_map: {self.brawler_mode_map}, "
                f"\nmode_map_brawler: {self.mode_map_brawler}, "
                f"\nmode_brawler: {self.mode_brawler}")

    def handle_battle(self, match_player_info, match_data):
        self.brawler_mode_map.handle_battle_result(match_player_info, match_data)
        self.mode_map_brawler.handle_battle_result(match_player_info, match_data)
        self.mode_brawler.handle_battle_result(match_player_info, match_data)
        # if hasattr(self, "brawler"):
        self.brawler.handle_battle_result(match_player_info, match_data)

class RecursiveStatCompiler(Serializable):
    def __init__(self, stat_chains):
        self.overall = ResultCompiler()
        self.stat_chain = stat_chains

        if self.stat_chain:
            # setattr(self, self.stat_chain[0], {})
            setattr(self, 'stat_map', {})
    
    def __str__(self):
        stat_map_str = ", ".join(f"{key}: {value}" for key, value in self.get_next_stat_map().items())
        return f"\nstat_chain: {self.stat_chain}, " \
               f"\noverall: {self.overall}, " \
               f"\nstat_map: {{{stat_map_str}}}"

    def get_next_stat_map(self):
        if not self.stat_chain:
            return {}

        # return getattr(self, self.stat_chain[0])
        try:
            return getattr(self, 'stat_map')
        except AttributeError:
            return getattr(self, self.stat_chain[0])

    def handle_battle_result(self, match_player_info, match_data):
        self.overall.handle_battle_result(match_player_info)

        if self.stat_chain:
            stat_map = self.get_next_stat_map()
            # stat_value = match_data[self.stat_chain[0]]
            stat_value = match_data.__getitem__(self.stat_chain[0])

            if stat_value not in stat_map:
                stat_map[stat_value] = RecursiveStatCompiler(self.stat_chain[1:])

            stat_map[stat_value].handle_battle_result(match_player_info, match_data)

class ResultCompiler(Serializable):

    def __init__(self):
        self.player_result_data = ResultTracker()
        self.player_star_data = ResultTracker()
    
    def __str__(self):
        return (
                f"\nplayer_result_data: {self.player_result_data}, "
                f"\nplayer_star_data: {self.player_star_data}, "
                )

    def handle_battle_result(self, match_player_info):

        self.player_result_data.__incrementitem__(match_player_info.result_type)
        self.player_result_data.potential_total += 1

        if match_player_info.star_player_exists:
            self.player_star_data.potential_total += 1

            if match_player_info.is_star_player:
                self.player_star_data.__incrementitem__(match_player_info.result_type)

class ResultTracker(Serializable):
    def __init__(self):
        self.wins = 0
        self.losses = 0
        self.draws = 0
        self.potential_total = 0
    
    def __str__(self):
        return f"\nwins: {self.wins}, losses: {self.losses}, draws: {self.draws}, potential_total: {self.potential_total}"
    
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)
    
    def __incrementitem__(self, key):
        setattr(self, key, getattr(self, key) + 1)

    def get_winrate(self):
        return self.wins / self.potential_total if self.potential_total > 0 else 0

    def get_formatted_winrate(self):
        w = self.get_winrate()
        if w == 0:
            return "-"
        return str(round(w * 100))

    def get_outcome_rate(self):
        return (self.wins + self.losses + self.draws) / self.potential_total if self.potential_total > 0 else 0

    def get_formatted_outcome_rate(self):
        w = self.get_outcome_rate()
        if w == 0:
            return "-"
        return str(round(w * 100))

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

class MatchPlayerInfo(Serializable):
    def __init__(self, result_type, is_star_player, star_player_exists):
        self.result_type = result_type  # Type of result (e.g., 'wins', 'losses')
        self.is_star_player = is_star_player  # Whether the player is the star player
        self.star_player_exists = star_player_exists  # Whether there is a star player

class MatchData(Serializable):
    def __init__(self, map, mode, brawler):
        self.map = map  # The map of the match
        self.mode = mode  # The mode of the match
        self.brawler = brawler  # The brawler used in the match

        if self.map == None:
            self.map = "unknown"
    
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

class MatchData():
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

    def __setitem__(self, key, value):
        setattr(self, key, value)
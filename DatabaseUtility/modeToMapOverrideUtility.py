
_mapToModeOverrides = {
    "Moonbark Meadow": "dodgeBrawl",
    "Rebound Ring": "dodgeBrawl",
    "Hug or Hurl": "dodgeBrawl",
    "Side Hustle": "dodgeBrawl",
    "Squish Court": "dodgeBrawl",
    "Wispwillow Ward": "dodgeBrawl",

    "Arena of Glory": "brawlArena",
    "Mirage Arena": "brawlArena",
    "Knockout Grounds": "brawlArena",
    "The Smackdome": "brawlArena",

    "Super Center": "brawlHockey",
    "Slippery Slap": "brawlHockey",
    "Bouncy Bowl": "brawlHockey",
    "Below Zero": "brawlHockey",
    "Cool Box": "brawlHockey",
    "Starr Garden": "brawlHockey",

    "Snowcone Square": "brawlHockey5V5",
    "Massive Meltdown": "brawlHockey5V5",
    "Frostbite Rink": "brawlHockey5V5",
    "Cold Snap": "brawlHockey5V5",

    "Divine Descent": "spiritWars",
    "Final Frontier": "spiritWars",
    "Celestial Crusade": "spiritWars",
    "Radiant Rampage": "spiritWars",
    "Hellish Harvest": "spiritWars",
    "Infernal Invasion": "spiritWars",
    "Abyssal Assault": "spiritWars",
    "Underworld Uprising": "spiritWars",

    "Foursquare Fortress": "soulCollector",
    "Hoop Boot Hill": "soulCollector",
    "Afterpiece Arena": "soulCollector",
    "Paperback Pond": "soulCollector",
    "Broiler Room": "soulCollector",
    "Kooky Gates": "soulCollector",
}

def getMode(game):
    if 'map' in game['event'] and game['event']['map'] in _mapToModeOverrides:
        return _mapToModeOverrides[game['event']['map']]
    elif 'mode' in game['event'] and game['event']['mode'] != "unknown":
        return game['event']['mode']
    elif 'mode' in game['battle']:
        return game['battle']['mode']
    else:
        return "unknown"
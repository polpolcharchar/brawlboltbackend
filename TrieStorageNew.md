
# Efficiently Storing Tries in DynamoDB

## Format

Each trie node is its own item:

- **Primary Key** path (trie#soloShowdown.Cavern Churn.BULL)
- **Sort Key** version/datetime
- List of children nodes primary keys
- Node value result/star data (could be stringified json or DynamoDB format)

Given a game, generate all paths that will need to be updated. For ranked soloShowdown CavernChurn BULL, this would look like:

- ""
- .ranked
- .ranked.BULL
- .ranked.BULL.soloShowdown
- .ranked.BULL.soloShowdown.CavernChurn
- .ranked.soloShowdown
- .ranked.soloShowdown.CavernChurn
- .ranked.soloShowdown.CavernChurn.BULL
- .ranked.soloShowdown.BULL

## Fetching

In all of these, the last attribute is the variable attribute
Simply fetch down the path of what is provided, and return the children nodes

TypeModeMapBrawler:

- Provide type, see children modes
- Provide type + mode, see children maps
- Provide type + mode + map, see children brawlers

TypeBrawlerModeMap:

- Provide type, see children brawlers
- Provide type + brawler, see children modes
- Provide type + brawler + mode, see children maps

TypeModeBrawler

- (repeat) Provide type, see children modes
- Provide type + mode, see children brawlers

It's also possible to have the variable attribute not be the end of the path
I think this will only apply to type

TypeModeMapBrawler:

- Provide mode, see different types
- Provide mode + map, see different types
- Provide mode + map + brawler, see different types

TypeBrawlerModeMap:

- Provide brawler, see differnet types
- Provide brawler + mode, see different types
- Provide brawler + mode + map, see different types

TypeModeBrawler:

- (repeat) Provide mode, see diff types
- (repeat to bmm) Provide mode + brawler, see diff types

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

## Utility Functions

```
saveTrie(gameAttributeTrie, currentPath="")

saveTrieNode(resultCompiler, pathID, childrenPathIDs)
```
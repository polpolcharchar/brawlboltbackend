# BrawlBolt Backend

[brawlbolt.com](https://www.brawlbolt.com/) is a free, open source account tracker for Brawl Stars. It tracks every game you play and provides instant access to statistics more detailed than any other site. Also, global statistics allow for analysis of the overall meta for any brawler or mode.

This repository is a collection of utility and main scripts used to automate the functions of [brawlbolt.com](https://www.brawlbolt.com/). It serves multiple functions, and it has [multiple main scripts](#Major-Deployments). [BrawlBolt statistic compilation](#brawlbolt-statistic-compilation) allows for statistics to be available to the user in the same time, no matter what.

## Major Deployments:

This repository handles website requests, player tracking, statistic compiling, and global statistics. These functions are explained in detail as follows:

### Website Backend:

[lambda_function.py](lambda_function.py) is deployed to AWS Lambda via [AWS CodePipeline](https://aws.amazon.com/codepipeline/) and handles requests from [brawlbolt.com](https://www.brawlbolt.com/). In order to handle global statistics, player statistics, and account tracking, it utilizes much of this repository.
It is able to handle requests for:

- Recent global statistics: the most recently compiled winrates of each brawler in each mode
- Past brawler-mode-level-specific global statistics: the winrates of Shelly in Brawl Ball over time
- Player data:
  - If the requested account is new to BrawlBolt, it will begin being tracked and have its most recent games compiled and statistics returned
  - If the requested account is already tracked, its compiled statistics will be returned

To learn more about how the client interacts with this data, see the [BrawlBolt Web Repository](https://github.com/polpolcharchar/brawlbolt).

### Compiler:

[compiler.py](compiler.py) is run every 48 hours to compile the statistics of all uncached games. It retrieves the currently-compiled statistics, adds the data from any uncached games, marks these games as cached, and saves the newly-compiled statistics.

### Tracker:

[tracker.py](tracker.py) is run every hour to check for newly-played games. The [Brawl Stars API](https://developer.brawlstars.com/#/) provides access to each player's most recent 25 matches. Any game from the API that is newer than BrawlBolt's most recent game is saved by BrawlBolt. If a player plays more than 25 games over 60 minutes, there is a chance some of the games will be lost.

### Global Statistics:

[Global Utility Functions](DatabaseUtility/globalUtility.py) are present in this repository. They handle data from BrawlBolt databases. The compiler that calculates these statistics is not public.


## BrawlBolt Statistic Compilation:

### Overview:

With access to all of a player's games, it is possible to calculate any statistic. Just loop over each game, tracking the desired statistic as you go. However, what happens when a player has tracked 1,000 games? 1,000,000? As the number of tracked games increases, so too does the time it will take to compile. BrawlBolt compiles these statistics in a format that balances speed, memory, and precision. By compiling and saving data from games as they are played, the user will never be waiting, no matter how many games they have played.

### What Data Should be Compiled?

For each match, the Brawl Stars API provides:

> - Time
> - Player Data:
>   - Name
>   - Tag
>   - Brawler
>   - Trophies/Rank
> - Result: win/loss/draw (if applicable)
> - Showdown Rank (if applicable)
> - Star Player (if applicable)
> - Trophy Change (if applicable)
> - Duration (if applicable)

With this, BrawlBolt compiles the following statistics, as seen in [Result Tracker](/CompilerStructuresModule/CompilerStructures/resultTracker.py) and [Player Result Compiler](/CompilerStructuresModule/CompilerStructures/playerResultCompiler.py):

> - Winrate
> - Star Rate (the percentage of time a player gets Star Player)
> - Trophy Change
> - Trophy Change / Game
> - Showdown Rank Distributions
> - Duration Distributions

Technically, it is possible to calculate more detailed statistics, like the winrate of two players together or the winrate of one certain brawler when they aren't playing against another certain brawler. However, this wouldn't be very useful and it would be much more difficult to implement.

### "Where" Should Data be Compiled?

In the previous section, six statistics to compile were established. These could just be compiled once per account, but that wouldn't be very useful. What is much more useful is knowing these statistics per brawler, per map, per mode, or any combination of the three. BrawlBolt defines four attributes of a game that are used for more specific compilation:

- Brawler
- Mode
- Map
- Regular vs Ranked

It is easy to access these values for each game, and it isn't too difficult to calculate statistics for each brawler, or each mode. But more specificy is definitely needed. The winrates for a brawler will be very different between regular games and ranked games. Thus, it is necessary to access statistics for combinations of these attributes. Despite greatly increasing complexity, BrawlBolt compiles statistics for any combination of these attributes (except global statistics, which do not include map).

### Statistic Format

It would be possible to store all possible combinations of attributes, but this would be very inefficient. Statistic values are only created for areas that apply to the player (wins for Spike are not tracked until a player has played Spike), and they are saved recursively. The [Game Attribute Trie](/CompilerStructuresModule/CompilerStructures/gameAttributeTrie.py) structure enables combinations of attributes. For player statistics, there are three recursive attribute paths: mode, map, brawler; brawler, mode, map; and mode, brawler. This means that it is possible to get statistics for any combination of mode, map, and brawler values.

## Dependencies:

This repository relies on multiple Python dependencies. In most cases, it is assumed that these are present on the current machine. For AWS Lambda, dependencies are available using a [Lambda Layer](https://docs.aws.amazon.com/lambda/latest/dg/chapter-layers.html). As of now, the dependencies are available in [requirements.txt](requirements.txt) and listed here:
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [requests](https://pypi.org/project/requests/)

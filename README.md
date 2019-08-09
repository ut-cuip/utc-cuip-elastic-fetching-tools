# utc-cuip-elastic-fetching-tools

This repository is intended as a "How-to" for grabbing bulk data from ElasticSearch efficiently. The ES Client for Python has some delay issues when generating a large query (such as a month's worth of data), so this repo shows you how to do that **efficiently**. Before the most recent changes (which were untracked by GitHub ☹️), the code for fetching a month of data on a single process and a single query took over a week to get less than a tenth of all of the data. With this new approach, it took three hours.

## Key Takeaways

- Break up a large query into many smaller ones: I did it by the *hour*.

- Multiprocess if possible: This code uses half of your systems total processors (including threads) to process your query in parallel. I use `pool` from `concurrent.futures` to do this without having to split up the data myself

## Useful Notes

Run either of these with the `--debug` switch to see exactly how many entries are being processed per second, and how many have been processed by that process overall.

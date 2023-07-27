# Setup

This project scrapes the youtube search using [scrapy](https://scrapy.org/) and [scrape-playwright](https://github.com/scrapy-plugins/scrapy-playwright).

To setup playwright, execute the following command:

```bash
playwright install
```

# Run

The folder `spiders` contains two different scrapers: videos and the channels. The output for the last run of the scrapers can be found in the respective `output-<NAME>.json`.

To re-execute a scraper, execute the following command:

```bash
scrapy crawl <NAME> -o output-<NAME>.json
```

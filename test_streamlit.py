"""
# My first app
Here's our first attempt at using data to create a table:
"""

from c4v.scraper.scraped_data_classes.scraped_data import ScrapedData
import pandas as pd
import c4v.microscope as ms

manager = ms.Manager.from_default()

def to_valid_row(data : ScrapedData) -> ScrapedData:
    if data.label:
        data.label = data.label.value

    if data.source:
        data.source = data.source.value

    return data
df = pd.DataFrame( [to_valid_row(x) for x in manager.get_all()][:1000] )

df
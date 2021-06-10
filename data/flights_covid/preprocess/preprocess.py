from pathlib import Path
import pandas as pd

flights = pd.concat(
    pd.read_csv(file, parse_dates=["firstseen", "lastseen", "day"])
    for file in Path("../dataset").glob("flightlist_*.csv.gz")
)
flights_dropna = flights.dropna(axis=0, subset=['registration', 'typecode','origin','destination', 'altitude_2'])
print(flights_dropna.head(10))
print(flights_dropna.isna().sum())
print(len(flights_dropna))

for col in flights_dropna.columns.values:
    print("%s's cardinality: "%col)
    print(len(flights_dropna[col].unique()))

flights_dropna.to_csv('../fligths_covid.csv', index=False)
print('done.')
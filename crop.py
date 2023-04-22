import pandas as pd

# Reading csv file

air_data = pd.read_csv("bristol-air-quality-data.csv",  sep=";")

# cropping data before january 2010
df = air_data.loc[air_data["Date Time"] >= "2010-01-01"].index
a = air_data.loc[df]

print(len(a))

# index=false ignores the indexing.
a.to_csv("crop.csv", sep=";")


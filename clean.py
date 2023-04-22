import pandas as pd

# pandas was imported to read the crop.csv file
new_data = pd.read_csv("crop.csv", sep=";")

# list of dictionary that shows the right station with street ID and street name
street = [

    {'street_id': 188, 'street_name': 'AURN Bristol Centre'},
    {'street_id': 203, 'street_name': 'Brislington Depot'},
    {'street_id': 206, 'street_name': 'Rupert Street'},
    {'street_id': 209, 'street_name': 'IKEA M32'},
    {'street_id': 213, 'street_name': 'Old Market'},
    {'street_id': 215, 'street_name': 'Parson Street School'},
    {'street_id': 228, 'street_name': 'Temple Meads Station'},
    {'street_id': 270, 'street_name': 'Wells Road'},
    {'street_id': 271, 'street_name': 'Trailer Portway P&R'},
    {'street_id': 375, 'street_name': 'Newfoundland Road Police Station'},
    {'street_id': 395, 'street_name': "Shiner's Garage"},
    {'street_id': 452, 'street_name': 'AURN St Pauls'},
    {'street_id': 447, 'street_name': 'Bath Road'},
    {'street_id': 459, 'street_name': 'Cheltenham Road \ Station Road'},
    {'street_id': 463, 'street_name': 'Fishponds Road'},
    {'street_id': 481, 'street_name': 'CREATE Centre Roof'},
    {'street_id': 500, 'street_name': 'Temple Way'},
    {'street_id': 501, 'street_name': 'Colston Avenue'},

]

#Filtering and remover dud records where there is no value for SiteID or there is a mismatch between SiteID and Location.


new_list = []
for lines, row in new_data.iterrows():
    if not any(street['street_name'] == row.Location
               and street["street_id"] == row.SiteID for street in street):
        print(lines+2, row.Location, row.SiteID)
    else:
        new_list.append(row)
new_df = pd.DataFrame(new_list)
new_df.to_csv('clean.csv', sep=";", index=False)
print(len(new_df ))
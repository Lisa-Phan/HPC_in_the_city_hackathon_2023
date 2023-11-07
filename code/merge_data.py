"""
Unfinished code to do normalization for count estimates on 
safegraph mobility data

Lisa P: 11/7/23
"""
import os
import pandas as pd
import json

MAIN_DIR = r"/work2/projects/utprojections/safegraph_data"

Census_data = os.path.join(MAIN_DIR,"OPEN_CENSUS_DATA", "safegraph_open_census_data_2018", "data", "cbg_b01.csv")
Pattern_data = os.path.join(MAIN_DIR, 'events.csv')


############### FUNCTION ####################
##### Read files and return dictionary ######
def cbg_to_population(Census_data = Census_data):
    """
    create dictionary to map cbg to population
    """
    cbg_01 = pd.read_csv(Census_data)
    group_to_population_map = dict(zip(cbg_01['census_block_group'].astype(str), cbg_01["B01001e1"]))
    return group_to_population_map

# Retire    
# def cbg_to_home_panel_device_count(Home_panel_data = Home_panel_data):
#     """
#     read home_panel data and retrieve home panel count for specified group
#     """
#     home_panel = pd.read_csv(Home_panel_data)
#     group_to_home_panel_map = dict(zip(home_panel['census_block_group'], home_panel['number_devices_residing']))
#     return group_to_home_panel_map


################ FUNCTION ################
def map_date_to_path(event_data):
    """
    Extract_date from event_data column date_range_start
    Get map it to the lower estimate of the week
    Use that estimate as a string to search
    """
    event_data['date'] = event_data['date_range_start'].str.split('T').str[0]
    event_data['date'] = pd.to_datetime(event_data['date'])

    Home_panel_dir = os.path.join(MAIN_DIR, 'FULL_MOBILITY', 'HOME_PANEL_SUMMARY')
    
    mapped_event_data = []

    for specific_date in event_data['date']:
        guess_file = '{}-home_panel_summary.csv'.format(specific_date.strftime('%Y-%m-%d'))
        guess_path = os.path.join(Home_panel_dir, guess_file)
        
        counter = 7
        
        while not os.path.isfile(guess_path) and counter > 0:
            print('File not found, searching {}'.format(guess_path))
            specific_date = specific_date - pd.Timedelta(days=1)
            guess_file = '{}-home_panel_summary.csv'.format(specific_date.strftime('%Y-%m-%d'))
            guess_path = os.path.join(Home_panel_dir, guess_file)
            counter -= 1

        if os.path.isfile(guess_path):
            mapped_event_data.append(guess_path)
        else:
            print('File not found for date {}, counter reached 0'.format(specific_date.strftime('%Y-%m-%d')))
            mapped_event_data.append(None)

    event_data['home_panel_path'] = mapped_event_data
    return event_data

def get_date_dictionary(event_data):
    """
    Read home_panel_path into a dictionary mapping cbs to sample
    """ 
    #list of dataframes
    home_panel_data = event_data['home_panel_path'].apply(lambda x: pd.read_csv(x))

    #list of dictionaries
    home_panel_data = home_panel_data.apply(lambda x: dict(zip(x['census_block_group'], x['number_devices_residing'])))
    
    #make this a column
    event_data['device_count_dictionary'] = home_panel_data

    return event_data

######## Calculate scale factor ##########
# Rewrite to iterrows
def get_scale_factor(event_data: pd.DataFrame, group_to_population_map = None):
    """
    Given an event data frame, 
    caclulate scale factor for each group in visitor_home_cbgs
    return the summed scale factor

    scale_factor_cbg = population_cbg/number_device_residing_cbg
    """
    new_scale_factor = []
    key_not_found = []

    scale_factor_dictionary = {}
    for row in event_data.iterrows():
        json = row[1]['visitor_home_cbgs']
        phone_dictionary = row[1]['device_count_dictionary']

        print('home_cbgs: ', json)
        print('phone_dictionary: ', phone_dictionary)



        for key in json:
            try:
                print('Processing {}'.format(key))
                population = group_to_population_map[key]
            except KeyError:
                print('Key {} not found for population'.format(key))
                key_not_found.append(key)
            else:
                try:
                    home_panel = phone_dictionary[key]
                except KeyError:
                    print('Key {} not found for home panel'.format(key))
                    key_not_found.append(key)
            scale = int(population)/int(home_panel)
            scale_factor_dictionary[key] = scale
        new_scale_factor.append(scale_factor_dictionary)

    #create dataframe with new column sum_scale_factor
    event_data['scale_factor_dictionary'] = new_scale_factor
    return event_data, key_not_found

def get_scaled_visitor_count(event_data: pd.DataFrame):
    """
    Use 'visitor_home_cbg' and 'scaled_factor_dictionary' to calculate scaled_visitor_count
    """
    for row in event_data.iterrows():

        visitor_count_column = []
        sum_pop = 0

        home_cbg = row[1]['visitor_home_cbgs']
        scale_factor_dictionary = row[1]['scale_factor_dictionary']
        for key in home_cbg:
            try:
                scale_factor = scale_factor_dictionary[key]
            except KeyError:
                print('Key {} not found for scale factor'.format(key))
                continue
            scaled_visitor_count = int(home_cbg[key]) * float(scale_factor)
            print(scaled_visitor_count)
            sum_pop += scaled_visitor_count
        visitor_count_column.append(sum_pop)
    event_data['scaled_visitor_count'] = scaled_visitor_count
    return event_data

# wrong
# def get_raw_visits_to_raw_visitor_ratio(event_dataframe: pd.DataFrame):
#     """
#     each two column is an integer, sum and get ratio
#     """
#     event_dataframe['visit_to_visitor'] = event_dataframe['raw_visit_counts']/event_dataframe['raw_visitor_counts']
#     return event_dataframe

# wrong
# def get_true_visit_estimate(event_dataframe):
#     """
#     multiply the scale factor to the raw_visit_counts
#     """
#     event_dataframe['true_visit_estimate'] = event_dataframe['raw_visit_counts'] * event_dataframe['sum_scale_factor']
#     return event_dataframe


############## APPLY FUNCTION ################
############## Create the dictionaries ##############
group_to_population_map = cbg_to_population(Census_data)

############## Read event file and load column population_cbg as json ##############
event_dataframe = pd.read_csv(Pattern_data)

#extract the json from visitor_home_cbgs
event_dataframe['visitor_home_cbgs'] = event_dataframe['visitor_home_cbgs'].apply(lambda x: json.loads(x))

#make new column home_panel_path
event_dataframe = map_date_to_path(event_dataframe)

#get device count dictionary
event_dataframe = get_date_dictionary(event_dataframe)

#calculate scale factor
event_dataframe, key_not_found = get_scale_factor(event_dataframe, group_to_population_map)

#calculate scaled_visitor_count
event_dataframe = get_scaled_visitor_count(event_dataframe)

#TODO: write code for getting actual estimates of the data




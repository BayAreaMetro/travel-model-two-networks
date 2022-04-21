import argparse
import pandas as pd
import numpy as np
from pyarrow import feather
from textdistance import levenshtein

USAGE = """"
    Reads a feather of roadway links and a csv of conflation results to create 
    a series of lasso attribute lookup files.
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("links_file_path",                help="Location of the links file (feather)")
    parser.add_argument("conflation_file_path",           help="Location of the conflation file (csv)")
    parser.add_argument("lasso_output_dir",               help="Directory for the output files")
    args = parser.parse_args()

    #Remote I/O, Data Reads
    links_df = pd.read_feather(args.links_file_path)
    conflation_df = pd.read_csv(args.conflation_file_path, low_memory=False)
    print("-- Data read successful")

    output_legacy_tm2_file_name = args.lasso_output_dir + "legacy_tm2_attributes.csv"
    output_tam_tm2_file_name = args.lasso_output_dir + "tam_tm2_attributes.csv"
    output_pems_file_name = args.lasso_output_dir + "pems_attributes.csv"
    output_sfcta_file_name = args.lasso_output_dir + "sfcta_attributes.csv"
    output_tomtom_file_name = args.lasso_output_dir + "tomtom_attributes.csv"
    output_osm_file_name = args.lasso_output_dir + "osm_lanes_attributes.csv"

    #Parameters
    frc_labels = ["Not Applicable", "Motorway, Freeway, or Other Major Road", 
              "Major Road Less Important than a Motorway", "Other Major Road",
              "Secondary Road", "Local Connecting Road",
              "Local Road of High Importance", "Local Road",
              "Local Road of Minor Importance", "Other Road"]

    ft_labels = ["Connector", "Freeway to Freeway",
                "Freeway", "Expressway",
                "Collector", "Ramp",
                "Special Facility", "Major Arterial"]

    frc_dict_df = pd.DataFrame({'code' : list(range(-1,9)), 'label' : frc_labels}) 
    ft_dict_df = pd.DataFrame({'code' : list(range(0,8)), 'label' : ft_labels})

    #OSM Dataframe
    osm_df = links_df[links_df.drive_access == 1]
    osm_df = osm_df[['shstReferenceId','lanes']]
    osm_df = osm_df.rename(columns={'lanes':'lanes_str'})

    osm_df['min_lanes'] = np.nan
    osm_df.loc[osm_df['lanes_str'].str.contains('12'), 'min_lanes'] = 12
    osm_df.loc[osm_df['lanes_str'].str.contains('11'), 'min_lanes'] = 11
    osm_df.loc[osm_df['lanes_str'].str.contains('10'), 'min_lanes'] = 10
    osm_df.loc[osm_df['lanes_str'].str.contains('9'), 'min_lanes'] = 9
    osm_df.loc[osm_df['lanes_str'].str.contains('8'), 'min_lanes'] = 8
    osm_df.loc[osm_df['lanes_str'].str.contains('7'), 'min_lanes'] = 7
    osm_df.loc[osm_df['lanes_str'].str.contains('6'), 'min_lanes'] = 6
    osm_df.loc[osm_df['lanes_str'].str.contains('5'), 'min_lanes'] = 5
    osm_df.loc[osm_df['lanes_str'].str.contains('4'), 'min_lanes'] = 4
    osm_df.loc[osm_df['lanes_str'].str.contains('3'), 'min_lanes'] = 3
    osm_df.loc[osm_df['lanes_str'].str.contains('2'), 'min_lanes'] = 2
    osm_df.loc[osm_df['lanes_str'].str.contains('1'), 'min_lanes'] = 1

    osm_df['max_lanes'] = osm_df['min_lanes']
    osm_df.loc[osm_df['lanes_str'].str.contains('2'), 'max_lanes'] = 2
    osm_df.loc[osm_df['lanes_str'].str.contains('3'), 'max_lanes'] = 3
    osm_df.loc[osm_df['lanes_str'].str.contains('4'), 'max_lanes'] = 4
    osm_df.loc[osm_df['lanes_str'].str.contains('5'), 'max_lanes'] = 5
    osm_df.loc[osm_df['lanes_str'].str.contains('6'), 'max_lanes'] = 6
    osm_df.loc[osm_df['lanes_str'].str.contains('7'), 'max_lanes'] = 7
    osm_df.loc[osm_df['lanes_str'].str.contains('8'), 'max_lanes'] = 8
    osm_df.loc[osm_df['lanes_str'].str.contains('9'), 'max_lanes'] = 9
    osm_df.loc[osm_df['lanes_str'].str.contains('10'), 'max_lanes'] = 10
    osm_df.loc[osm_df['lanes_str'].str.contains('11'), 'max_lanes'] = 11
    osm_df.loc[osm_df['lanes_str'].str.contains('12'), 'max_lanes'] = 12

    osm_df = osm_df.dropna()
    osm_df = osm_df[['shstReferenceId', 'min_lanes', 'max_lanes']]

    #OSM Names Dataframe
    osm_names_df = links_df[links_df.drive_access == 1]
    osm_names_df = osm_names_df[['shstReferenceId','name']]

    osm_names_df['name'] = osm_names_df['name'].str.replace("'","").str.replace("\\[","",regex=True).str.replace("\\]","",regex=True)
    names_split = osm_names_df['name'].str.split(',', n=5, expand=True)
    names_split = names_split.iloc[: , :-1]
    osm_names_df = osm_names_df[['shstReferenceId']]
    osm_names_df = pd.concat([osm_names_df, names_split], axis=1)
    osm_names_df = osm_names_df.melt(id_vars=['shstReferenceId'],value_vars=[*range(0,5)],
                                    var_name='index',value_name='name')

    osm_names_df = osm_names_df[['shstReferenceId','name']]
    osm_names_df['name'] = osm_names_df['name'].str.strip()
    osm_names_df = osm_names_df.dropna()
    osm_names_df = osm_names_df[osm_names_df.name != 'nan']
    osm_names_df = osm_names_df[osm_names_df.name != '']

    osm_names_df = osm_names_df.drop_duplicates()
    osm_names_df['index'] = osm_names_df.groupby(['shstReferenceId']).shstReferenceId.transform('cumcount')
    osm_names_df['index'] = osm_names_df['index'] + 1

    osm_names_df = osm_names_df.pivot(index='shstReferenceId', columns='index', values='name').add_prefix("name_")
    osm_names_df['name'] = osm_names_df['name_1']
    osm_names_df.loc[~osm_names_df['name_2'].isnull(), 'name'] = osm_names_df['name'] + '/' + osm_names_df['name_2']
    osm_names_df.loc[~osm_names_df['name_3'].isnull(), 'name'] = osm_names_df['name'] + '/' + osm_names_df['name_3']

    remove_cols = [' Avenue',' Street',' Drive',' Boulevard',' Way']
    osm_names_df['simple_name'] = osm_names_df['name'].str.replace('|'.join(remove_cols),'',regex=True)
    osm_names_df = osm_names_df.reset_index()
    osm_names_df = osm_names_df[['shstReferenceId','name','simple_name']]
    osm_names_df['simple_name'] = osm_names_df['simple_name'].str.strip()

    #Legacy Travel Model Network Dataframe
    legacy_df = conflation_df.dropna(subset=['TM2_A'])
    legacy_df = legacy_df[['shstReferenceId','TM2_A','TM2_B','TM2_FT',
                        'TM2_LANES', 'TM2_ASSIGNABLE']]

    legacy_df = legacy_df.rename(columns={'shstReferenceId':'shstReferenceId',
                                        'TM2_A':'A_node',
                                        'TM2_B':'B_node',
                                        'TM2_FT':'code', 
                                        'TM2_LANES':'lanes',
                                        'TM2_ASSIGNABLE':'assignable'})
    convert_dict = {'A_node': int,
                    'B_node': int,
                    'code': int,
                    'lanes': int,
                    'assignable': int}
    legacy_df = legacy_df.astype(convert_dict)
    legacy_df = legacy_df.merge(ft_dict_df, on='code', how='left')
    legacy_df = legacy_df.rename(columns={'label':'ft'})
    legacy_df = legacy_df.drop(['code'], axis=1)

    #Marin Dataframe
    tam_df = conflation_df.dropna(subset=['TM2Marin_A'])
    tam_df = tam_df[['shstReferenceId','TM2Marin_A','TM2Marin_B','TM2Marin_FT',
                        'TM2Marin_LANES', 'TM2Marin_ASSIGNABLE']]

    tam_df = tam_df.rename(columns={'shstReferenceId':'shstReferenceId',
                                        'TM2Marin_A':'A_node',
                                        'TM2Marin_B':'B_node',
                                        'TM2Marin_FT':'code', 
                                        'TM2Marin_LANES':'lanes',
                                        'TM2Marin_ASSIGNABLE':'assignable'})
    convert_dict = {'A_node': int,
                    'B_node': int,
                    'code': int,
                    'lanes': int,
                    'assignable': int}
    tam_df = tam_df.astype(convert_dict)
    tam_df = tam_df.merge(ft_dict_df, on='code', how='left')
    tam_df = tam_df.rename(columns={'label':'ft'})
    tam_df = tam_df.drop(['code'], axis=1)

    #PeMS Dataframe
    pems_cols = [col for col in conflation_df.columns if 'pems' in col] 
    pems_df = conflation_df[['shstReferenceId'] + ['PEMSID'] + pems_cols].copy()

    pems_df.loc[~pems_df['pems_lanes_FR'].isnull(), 'pems_ft'] = 'Ramp'
    pems_df.loc[~pems_df['pems_lanes_OR'].isnull(), 'pems_ft'] = 'Ramp'
    pems_df.loc[~pems_df['pems_lanes_FF'].isnull(), 'pems_ft'] = 'Freeway to Freeway'
    pems_df.loc[~pems_df['pems_lanes_ML'].isnull(), 'pems_ft'] = 'Freeway'
    pems_df.loc[~pems_df['pems_lanes_HV'].isnull(), 'pems_ft'] = 'Freeway'

    pems_df.loc[~pems_df['pems_lanes_FR'].isnull(), 'pems_lanes'] = pems_df['pems_lanes_FR']
    pems_df.loc[~pems_df['pems_lanes_OR'].isnull(), 'pems_lanes'] = pems_df['pems_lanes_OR']
    pems_df.loc[~pems_df['pems_lanes_FF'].isnull(), 'pems_lanes'] = pems_df['pems_lanes_FF']
    pems_df.loc[~pems_df['pems_lanes_ML'].isnull(), 'pems_lanes'] = pems_df['pems_lanes_ML']
    pems_df.loc[~pems_df['pems_lanes_HV'].isnull(), 'pems_lanes'] = pems_df['pems_lanes_HV'] + pems_df['pems_lanes']

    pems_df = pems_df.drop(pems_cols, axis=1)
    pems_df = pems_df.rename(columns={'pems_ft':'ft', 'pems_lanes':'lanes'})
    pems_df = pems_df.dropna()
    pems_df['lanes'] = pems_df['lanes'].astype('int')

    #TomTom Dateframe
    tom_df = conflation_df.dropna(subset=['tomtom_unique_id'])
    tom_df = tom_df[['shstReferenceId','tomtom_unique_id','tomtom_FRC','tomtom_lanes',
                        'tomtom_shieldnum', 'tomtom_rtedir']]

    tom_df = tom_df.rename(columns={'tomtom_unique_id':'tom_id',
                                        'tomtom_FRC':'code',
                                        'tomtom_lanes':'lanes'})
    convert_dict = {'code': int,
                    'lanes': int}
    tom_df = tom_df.astype(convert_dict)
    tom_df = tom_df.merge(frc_dict_df, on='code', how='left')
    tom_df = tom_df.rename(columns={'label':'ft'})
    tom_df = tom_df.drop(['code'], axis=1)

    tom_df.loc[tom_df['tomtom_shieldnum']==' ', 'tomtom_shieldnum'] = 'NA'
    tom_df.loc[tom_df['tomtom_rtedir']==' ', 'tomtom_rtedir'] = 'NA'

    tom_df['name'] = tom_df['tomtom_shieldnum'] + [' '] + tom_df['tomtom_rtedir']
    tom_df = tom_df.drop(['tomtom_shieldnum', 'tomtom_rtedir'], axis=1)

    #SFCTA Dataframe
    sfcta_df = conflation_df.dropna(subset=['sfcta_A'])
    sfcta_df = sfcta_df[['shstReferenceId','sfcta_A','sfcta_B','sfcta_LANE_AM',
                        'sfcta_LANE_OP', 'sfcta_LANE_PM', 'sfcta_STREETNAME']]

    sfcta_df = sfcta_df.rename(columns={'sfcta_A':'A_node',
                                        'sfcta_B':'B_node',
                                        'sfcta_LANE_AM':'lanes_am',
                                        'sfcta_LANE_OP':'lanes_md',
                                        'sfcta_LANE_PM':'lanes_pm',
                                        'sfcta_STREETNAME':'name'})
    convert_dict = {'A_node': int,
                    'B_node': int,
                    'lanes_am': int,
                    'lanes_md': int,
                    'lanes_pm': int}
    sfcta_df = sfcta_df.astype(convert_dict)

    sfcta_df['min_lanes'] = sfcta_df[['lanes_am','lanes_md','lanes_pm']].min(axis=1)
    sfcta_df['max_lanes'] = sfcta_df[['lanes_am','lanes_md','lanes_pm']].max(axis=1)
    sfcta_df = sfcta_df.merge(osm_names_df[['shstReferenceId','simple_name']], 
                            on='shstReferenceId', how='left')
    sfcta_df['name'] = sfcta_df['name'].astype('str')
    sfcta_df['simple_name'] = sfcta_df['simple_name'].astype('str')
    sfcta_df['osm_sfcta_names_dist'] = sfcta_df.apply(lambda x: levenshtein.distance(x['name'].lower(),  
                                                x['simple_name'].lower()), axis=1)
    sfcta_df.loc[sfcta_df.name=='nan', 'osm_sfcta_names_dist'] = 100
    sfcta_df.loc[sfcta_df.simple_name=='nan', 'osm_sfcta_names_dist'] = 100
    sfcta_df[['name','simple_name']] = sfcta_df[['name','simple_name']].replace('nan', np.nan)
    sfcta_df = sfcta_df.drop(['simple_name'], axis=1)

    #Write Files to Output Directory
    legacy_df.to_csv(output_legacy_tm2_file_name)
    tam_df.to_csv(output_tam_tm2_file_name)
    pems_df.to_csv(output_pems_file_name)
    sfcta_df.to_csv(output_sfcta_file_name)
    tom_df.to_csv(output_tomtom_file_name)
    osm_df.to_csv(output_osm_file_name)

    print("-- Outputs complete")







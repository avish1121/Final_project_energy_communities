import pandas as pd
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
import io
import warnings
from matplotlib.colors import ListedColormap
warnings.filterwarnings("ignore") 
import requests
import sys

# Getting state names, abbreviations and fips 
abb_fips = pd.read_csv("data/us-state-ansi-fips.csv")
abb_fips1 = abb_fips.rename(columns={' st':'state_fips', ' stusps':'state_abb', 'stname':'state_name'})
abb_fips1['state_abb'] = abb_fips1['state_abb'].str.slice(start=1)
#abb_fips1.info()

# Input state

state = sys.argv[1]
#state = 'Texas'
# If condition to check state name

if str(state) in list(abb_fips1["state_name"].unique()):

    # Getting census tract level shape files
    us_census = gpd.read_file("data/us_census_tracts_20210707.jsonl")
    #us_census.head() 
    # Removing puerto rico from the list
    us_census1 = us_census.loc[us_census["state_fips"]!='72', [ "state_fips","tract_id","geometry","name","county_fips" ]]
    us_census2 = us_census1.astype({"tract_id":"int64","state_fips":"int64"})
    #us_census2.info()

    merged_census = pd.merge(left=us_census2, right=abb_fips1,on="state_fips", how="inner")

    state_merged_census = merged_census.loc[(merged_census['state_name']== str(state)),["state_fips","tract_id","geometry","name","county_fips"] ]
    #state_merged_census.head()
    #state_merged_census.plot()
    #array(abb_fips1["state_abb"])
    state_merged_census["coalmine"] = 0
    state_merged_census['coal_adj_tract'] = 0
    state_merged_census['coal_energy_comm'] = 0
    state_merged_census["coalPP"] = 0
    state_merged_census['coalPP_adj_tract'] = 0
    state_merged_census['coalPP_energy_comm'] = 0

    # Identifying coal communities

    # Areas with closed coal mines

    data_coalmine=pd.read_csv("data/Mines.txt", sep= "|", encoding= "latin-1" )
    #data_coalmine.columns
    #data_coalmine["CURRENT_MINE_TYPE"].unique()
    data_cm = data_coalmine[["MINE_ID","COAL_METAL_IND","CURRENT_MINE_NAME","CURRENT_MINE_TYPE","CURRENT_MINE_STATUS", "FIPS_CNTY_CD","FIPS_CNTY_NM","CURRENT_STATUS_DT", 'STATE','LONGITUDE', 'LATITUDE']]
    #data_cm["COAL_METAL_IND"].unique()
    data_cm["CURRENT_STATUS_DT"] = pd.to_datetime(data_cm["CURRENT_STATUS_DT"])
    data_cm_abbv = data_cm.rename(columns={'STATE':'state_abb'})
    data_cm_abbv_r = data_cm_abbv.loc[~data_cm_abbv["state_abb"].isin(['MP','PR','GU','VI'])]
    data_cm_abbv_r =data_cm_abbv_r.astype({"state_abb":"str"})
    abb_fips1 =abb_fips1.astype({"state_abb":"str"})

    data_cm_abbv_merge = pd.merge(left=data_cm_abbv_r, right=abb_fips1,on="state_abb", how="left")
    #data_cm_abbv_merge.head(50000)

    #dictabb = abb_fips1.set_index("stname")['state_abb'].to_dict()
    #stateabb = dictabb[state]

    coalmines_state = data_cm_abbv_merge[(data_cm_abbv_merge['state_name'] == state) & (data_cm_abbv_merge["COAL_METAL_IND"] == "C") & ((data_cm_abbv_merge["CURRENT_MINE_TYPE"] == 'Surface') | (data_cm_abbv_merge["CURRENT_MINE_TYPE"] == 'Underground'))]

    coalmines_state_abnd  = coalmines_state[(coalmines_state["CURRENT_MINE_STATUS"] == 'Abandoned and Sealed') | (coalmines_state["CURRENT_MINE_STATUS"] == 'Abandoned') | (coalmines_state["CURRENT_MINE_STATUS"] == 'NonProducing')]
    coalmines_state_abnd_2000 = coalmines_state_abnd[(coalmines_state_abnd["CURRENT_STATUS_DT"].dt.year > 1999) ]
    #coalmines_state_abnd_2000.info()
    coalmines_state_abnd_2000.dropna()
    if len(coalmines_state_abnd_2000["MINE_ID"])>0: 
        url = "https://geo.fcc.gov/api/census/block/find"

        coalmine_locations = coalmines_state_abnd_2000[['LATITUDE','LONGITUDE']]
        coalmine_locations = coalmine_locations.assign(
            block=coalmine_locations.apply(
                lambda r: requests.get(
                    url, params={"latitude": r["LATITUDE"], "longitude": r["LONGITUDE"], "format": "json"}
                ).json()["Block"]["FIPS"],
                axis=1,
            )
        )

        coalmine_locations["tract_id"] = coalmine_locations['block'].astype(str).str[:-4]
        coalmine_locations["tract_id"] = coalmine_locations["tract_id"].astype(str).str[:11]
        coalmine_locations = coalmine_locations[coalmine_locations["tract_id"].str.len() >10 ]
        coalmine_locations.dropna()
        #list(coalmine_locations["tract_id"])
        coalmine_locations = coalmine_locations.astype({"tract_id":"int64"})
        #coalmine_locations["tract_id"] = coalmine_locations["tract_id"].astype("float64")
        #coalmine_locations.info()
        #energy_com_coal = state_merged_census[["tract_id"]]
        #len(state_merged_census["tract_id"].unique())
        #energy_com_coal = pd.merge(state_merged_census,coalmine_locations,on = "tract_id", how = "left")
        #energy_com_coal
        #state_merged_census.columns
        
        tractlist = list(coalmine_locations["tract_id"])
        
        for index, row in state_merged_census.iterrows():
            if row['tract_id'] in tractlist:
                state_merged_census.at[index,"coalmine"] = 1    
        energy_com_coal = state_merged_census[state_merged_census["coalmine"] == 1]
        adjacent_tract = gpd.sjoin(state_merged_census,energy_com_coal,how = "inner", op = 'touches')
        #adjacent_tract.plot()

        adj_tractlist = list(adjacent_tract["tract_id_left"])
        
        for index, row in state_merged_census.iterrows():
            if row['tract_id'] in adj_tractlist:
                state_merged_census.at[index,'coal_adj_tract'] = 1 
        state_merged_census["coal_energy_comm"] = state_merged_census["coalmine"] + state_merged_census['coal_adj_tract']
        #state_merged_census.describe()
        #state_merged_census.loc[state_merged_census.index != adjacent_tract.index, 'ADJACENT TRACT'] = 1
        #colors = {"coalmine":"Red",'coal_adj_tract':"Blue"}
        #cmap = ListedColormap(colors)
        state_merged_census["coal_energy_comm"] = np.where(state_merged_census["coal_energy_comm"]>1,1,state_merged_census["coal_energy_comm"])
        #state_merged_census.plot(column="coal_energy_comm")
        #print("This Map shows energy communities with closed coal mines since 2000 in " + str(state))
        #plt.show()


        fig, ax = plt.subplots(figsize=(10, 6))
        state_merged_census.plot(column="coal_energy_comm", cmap='OrRd', ax = ax, alpha=1, edgecolor="grey") 
        #state_merged_census.plot(column='coal_adj_tract', cmap='Blues', ax= ax)

        # add a title and labels
        ax.set_title('This map shows energy communities with closed coal mines since 2000 in ' + str(state))
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

        # show the plot
        plt.show()
        ax.figure.savefig('Coal_mine_energy_communities.png')
    else:
        print("There are no closed coal mines in this state")


    # Identifying Retired coal fired power plants

    data_PP = pd.read_excel("data/december_generator2022.xlsx" , sheet_name="Retired", skiprows=2)
    #data_PP.columns
    data_PP_2010 = data_PP.loc[data_PP["Retirement Year"] > 2009, [ 'Energy Source Code','Plant State','Plant ID','Retirement Year', 'Latitude','Longitude' ]]
    data_PP_2010_coal = data_PP_2010.loc[data_PP_2010['Energy Source Code'].isin(["ANT", "BIT", "LIG", "SUB", "SGC", "WC", "RC"])]
    #data_PP_2010_coal['Energy Source Code'].unique()
    #data_PP_2010_coal.info()
    data_PP_2010_coal_abbv = data_PP_2010_coal.rename(columns={'Plant State':'state_abb'})

    PP_2010_coal_abbv_merge = pd.merge(left=data_PP_2010_coal_abbv, right=abb_fips1,on="state_abb", how="left")
    PP_coal_2010_state = PP_2010_coal_abbv_merge[(PP_2010_coal_abbv_merge['state_name'] == state)]
    #PP_coal_2010_state.info()
    PP_coal_2010_state.dropna()
    PP_coal_2010_state_r = PP_coal_2010_state.astype({"Latitude":"float64", "Longitude":"float64"})

    #PP_coal_2010_state_r.info()
    if len(PP_coal_2010_state_r['state_abb'])>0: 
        url = "https://geo.fcc.gov/api/census/block/find"

        coalPP_locations = PP_coal_2010_state_r[['Latitude','Longitude']]
        coalPP_locations = coalPP_locations.assign(
            block=coalPP_locations.apply(
                lambda r: requests.get(
                    url, params={"latitude": r["Latitude"], "longitude": r["Longitude"], "format": "json"}
                ).json()["Block"]["FIPS"],
                axis=1,
            )
        )

        coalPP_locations["tract_id"] = coalPP_locations['block'].astype(str).str[:-4]
        coalPP_locations["tract_id"] = coalPP_locations["tract_id"].astype(str).str[:11]
        coalPP_locations = coalPP_locations[coalPP_locations["tract_id"].str.len() >10 ]
        coalPP_locations.dropna()
        coalPP_locations = coalPP_locations.astype({"tract_id":"int64"})
        tractlist = list(coalPP_locations["tract_id"])
        
        for index, row in state_merged_census.iterrows():
            if row['tract_id'] in tractlist:
                state_merged_census.at[index,"coalPP"] = 1    
        energy_com_coalPP = state_merged_census[state_merged_census["coalPP"] == 1]
        adjacent_tract = gpd.sjoin(state_merged_census,energy_com_coalPP,how = "inner", op = 'touches')
        #adjacent_tract.plot()

        adj_tractlist = list(adjacent_tract["tract_id_left"])
    
        for index, row in state_merged_census.iterrows():
            if row['tract_id'] in adj_tractlist:
                state_merged_census.at[index,'coalPP_adj_tract'] = 1 
        state_merged_census["coalPP_energy_comm"] = state_merged_census["coalPP"] + state_merged_census['coalPP_adj_tract'] 
        #state_merged_census.describe()
        #state_merged_census.loc[state_merged_census.index != adjacent_tract.index, 'ADJACENT TRACT'] = 1
        #colors = {"coalmine":"Red",'coal_adj_tract':"Blue"}
        #cmap = ListedColormap(colors)
        state_merged_census["coalPP_energy_comm"] = np.where(state_merged_census["coalPP_energy_comm"]>1,1,state_merged_census["coalPP_energy_comm"])
        #state_merged_census.plot(column="coalPP_energy_comm")
        #print()
        fig, ax = plt.subplots(figsize=(10, 6))
        state_merged_census.plot(column="coalPP_energy_comm", cmap='OrRd', ax = ax, alpha=1, edgecolor="grey")
        #state_merged_census.plot(column='coal_adj_tract', cmap='Blues', ax= ax)

        # add a title and labels
        ax.set_title("This map shows energy communities with closed coal fired power plants since 2010 in " + str(state))
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        # show the plot
        plt.show()
        ax.figure.savefig('Coal_powerPlant_energy_communities.png')

    else:
        print("There are no closed coal-fired power plants in this state")

    state_merged_census["Comb_Energy_Comm"] = state_merged_census["coalPP_energy_comm"] + state_merged_census["coal_energy_comm"]
    state_merged_census["Comb_Energy_Comm"] = np.where(state_merged_census["Comb_Energy_Comm"]>1,1,state_merged_census["Comb_Energy_Comm"])
    # Combined plot
    #fig, ax = plt.subplots(figsize=(10, 6))
    #state_merged_census.plot(column="Comb_Energy_Comm", cmap='OrRd', ax = ax, alpha=1, edgecolor="grey")
    #state_merged_census.plot(column='coal_adj_tract', cmap='Blues', ax= ax)
    # add a title and labels
    #ax.set_title("This map shows combined energy communities with closed coal mine and coal fired power plants in " + str(state))
    #ax.set_xlabel('Longitude')
    #ax.set_ylabel('Latitude')
    # show the plot
    #plt.show()

    # Identifying brownfields for every state

    brownfields = gpd.read_file("data/brownfields/brownfields.shp")
    #brownfields.plot()

    merged_brownfield = gpd.sjoin(brownfields,state_merged_census, how="inner", op="within")
    if len(merged_brownfield['state_fips'])>0:
        fig, ax = plt.subplots(figsize=(10, 6))
        state_merged_census.plot(ax=ax,  alpha=1, edgecolor="grey")
        merged_brownfield.plot(ax=ax, color="red", markersize=10)
        ax.set_title("This map shows Brownfield energy communities in " + str(state))
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        # show the plot
        plt.show()
        ax.figure.savefig('Brownfield_energy_communities.png')
        #fig, ax = plt.subplots(figsize=(10, 6))
        #state_merged_census.plot(column="Comb_Energy_Comm", cmap='OrRd', ax = ax, alpha=1, edgecolor="grey")
        #merged_brownfield.plot(ax=ax, color="red", markersize=10)
        #ax.set_title("This map shows combined energy communities in " + str(state))
        #ax.set_xlabel('Longitude')
        #ax.set_ylabel('Latitude')
        #plt.show()
    else:
        print("There are no Brownfields in this state")

    # Identifying fossil fuel employment communities

    employment_data = gpd.read_file("data/MSA_NMSA_FFE_SHP/MSA_NMSA_FFE_SHP.shp")
    #employment_data.columns
    employment_data_state = employment_data[employment_data["state_name"]== state]
    #employment_data_state.plot()
    #merged_employment_data = pd.merge(state_merged_census,employment_data)
    #gpd.sjoin(employment_data,state_merged_census, how="left", op ='within')
    #merged_employment_data.plot()

    if len(employment_data_state['state_name'])>0:
        fig, ax = plt.subplots(figsize=(10, 6))
        state_merged_census.plot( ax = ax,color = 'none', alpha=0.9, edgecolor="grey")
        employment_data_state.plot(ax=ax,alpha=0.5, edgecolor="red")
        ax.set_title("This map shows energy communities ( MSA + Non-MSA regions) qualified under fossil fuel employment criteria in " + str(state))
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        plt.show()
        ax.figure.savefig('Fossilfuel_employment_energy_communities.png')
    else:
        print("There are no fossil fuel energy communities in this state")
   
    #COMBINED ENERGY COMMUNITY MAP OF STATE

    fig, ax = plt.subplots(figsize=(10, 6))
    if (len(PP_coal_2010_state_r['state_abb'])>0) | (len(PP_coal_2010_state_r['state_abb'])>0):
        state_merged_census.plot(column="Comb_Energy_Comm", cmap='OrRd', ax = ax, alpha=1, edgecolor="grey")
    if len(employment_data_state['state_name'])>0:
        employment_data_state.plot(ax=ax,alpha=0.4, edgecolor="black")
    if len(merged_brownfield['state_fips'])>0:
        if (len(PP_coal_2010_state_r['state_abb'])==0) & (len(PP_coal_2010_state_r['state_abb'])==0):
            if len(employment_data_state['state_name'])>0:
                state_merged_census.plot( ax = ax,color = 'none', alpha=0.9, edgecolor="grey")
                employment_data_state.plot(ax=ax,alpha=0.5, edgecolor="red")
                merged_brownfield.plot(ax=ax, color="red", markersize=10)
            else:
                state_merged_census.plot(ax=ax, color = 'none', alpha=0.9, edgecolor="grey")
                merged_brownfield.plot(ax=ax, color="red", markersize=10)
        else:
            merged_brownfield.plot(ax=ax, color="red", markersize=10)

    ax.set_title("This map shows combined energy communities in " + str(state))
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    plt.show()
    ax.figure.savefig('All_energy_communities_combined.png')

else:
    print( str(state) + " is not a state in the United States"+"\n"+ "Please enter a correct name and first letter cap")


















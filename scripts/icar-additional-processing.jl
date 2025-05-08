#%%
using DrWatson
@quickactivate "FMDData"

using FMDData
using Try

# "2019_Annual-Report_A-N-Islands.csv"
# "2019_Annual-Report_Andhra-Pradesh.csv"
# "2019_Annual-Report_Bihar.csv"
# "2019_Annual-Report_Chhattisgarh.csv"
# "2019_Annual-Report_Goa.csv"
# "2019_Annual-Report_Gujarat.csv"
# "2019_Annual-Report_Haryana.csv"
# "2019_Annual-Report_Jammu-Kashmir.csv"
# "2019_Annual-Report_Karnataka.csv"
# "2019_Annual-Report_Kerala.csv"
# "2019_Annual-Report_Madhya-Pradesh.csv"
# "2019_Annual-Report_Maharashtra.csv"
# "2019_Annual-Report_Manipur.csv"
# "2019_Annual-Report_Mizoram.csv"
# "2019_Annual-Report_Odisha.csv"
# "2019_Annual-Report_Pondicherry.csv"
# "2019_Annual-Report_Punjab.csv"
# "2019_Annual-Report_Rajasthan.csv"
# "2019_Annual-Report_Tamil-Nadu.csv"
# "2019_Annual-Report_Telangana.csv"
# "2019_Annual-Report_Uttar-Pradesh.csv"
# "2019_Annual-Report_Uttarakhand.csv"
# "2019_Annual-Report_West-Bengal.csv"
# "2020_Annual-Report_NADCP-1.csv"
# "2020_Annual-Report_Organized-farms.csv"
# "2021_Annual-Report_NADCP-1.csv"
# "2021_Annual-Report_NADCP-2.csv"
# "2021_Annual-Report_Organized-farms.csv"
# "2022_Annual-Report_NADCP-2.csv"
# "2022_Annual-Report_NADCP-3.csv"
# "2022_Annual-Report_Organized-farms.csv"

#%%
data = @? load_csv(
    "clean_2022_Annual-Report_Organized-farms.csv",
    datadir("icar-seroprevalence")
)

add_sample_year!(
    data,
    2022
)

#%%
nadcp_2_2022 = @? load_csv(
    "clean_2022_Annual-Report_NADCP-2.csv",
    datadir("icar-seroprevalence")
)

nadcp_2_2021 = @? load_csv(
    "clean_2021_Annual-Report_NADCP-2.csv",
    datadir("icar-seroprevalence")
)

#%%
n2022 = deepcopy(nadcp_2_2022)
n2021 = deepcopy(nadcp_2_2021)

add_sample_year!(
    n2022,
    n2021,
    2022,
    2021
)

n2022

#%%
calculate_state_seroprevalence(n2022)

match(
    Regex("serotype_(?:$(join(FMDData.default_allowed_serotypes, "|")))_count_(pre|post)"),
    "serotype_o_count_pre_calculated"
)


#%%
reg = Regex("serotype_(?:$(join(FMDData.default_allowed_serotypes, "|")))_count_(pre|post).*")
replace(
    "serotype_o_count_pre",
    reg => s"serotype_all_count_\1"
)

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
cumulative_nadcp_2_2022 = @? load_csv(
    "clean_2022_Annual-Report_NADCP-2.csv",
    icar_cleaned_dir()
)

nadcp_2_2021 = @? load_csv(
    "clean_2021_Annual-Report_NADCP-2.csv",
    icar_cleaned_dir()
)

nadcp_2_2022 = @? infer_later_year_values(
    cumulative_nadcp_2_2022,
    nadcp_2_2021,
)

add_sample_year!(
    cumulative_nadcp_2_2022 => "Combined",
    nadcp_2_2022 => 2022,
    nadcp_2_2021 => 2021
)

add_report_year!(
    cumulative_nadcp_2_2022 => 2022,
    nadcp_2_2022 => 2022,
    nadcp_2_2021 => 2021
)

add_round_name!(
    cumulative_nadcp_2_2022 => "NADCP 2",
    nadcp_2_2022 => "NADCP 2",
    nadcp_2_2021 => "NADCP 2"
)

add_test_type!(
    cumulative_nadcp_2_2022 => "SPCE",
    nadcp_2_2022 => "SPCE",
    nadcp_2_2021 => "SPCE"
)

add_test_threshold!(
    cumulative_nadcp_2_2022 => "1.65 log10 @ 50% inhibition",
    nadcp_2_2022 => "1.65 log10 @ 50% inhibition",
    nadcp_2_2021 => "1.65 log10 @ 50% inhibition"
)

nadcp_2 = combine_round_dfs(
    cumulative_nadcp_2_2022, nadcp_2_2022, nadcp_2_2021
)

FMDData.write_csv("nadcp_2_2022.csv", icar_processed_dir(), nadcp_2_2022)
FMDData.write_csv("nadcp_2_2021.csv", icar_processed_dir(), nadcp_2_2021)
FMDData.write_csv("nadcp_2.csv", icar_processed_dir(), nadcp_2)

#%%

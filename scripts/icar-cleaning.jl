#%%
using DrWatson
@quickactivate "FMDData"

using FMDData

#%%
for file in [
        "2022_Annual-Report_Organized-farms.csv",
        "2022_Annual-Report_NADCP-3.csv",
        "2022_Annual-Report_NADCP-2.csv",
        "2021_Annual-Report_Organized-farms.csv",
        "2021_Annual-Report_NADCP-2.csv",
        "2021_Annual-Report_NADCP-1.csv",
        "2020_Annual-Report_NADCP-1.csv",
    ]
    all_cleaning_steps(
        file,
        icar_inputs_dir(),
    )
end

#%%
# errors as provides farm-level data with duplicated states
# all_cleaning_steps(
#     "2020_Annual-Report_Organized-farms.csv",
#     icar_inputs_dir(),
# )

#%%
for file in [
        "2019_Annual-Report_A-N-Islands.csv"
        "2019_Annual-Report_Andhra-Pradesh.csv"
        "2019_Annual-Report_Bihar.csv"
        "2019_Annual-Report_Chhattisgarh.csv"
        "2019_Annual-Report_Goa.csv"
        "2019_Annual-Report_Gujarat.csv"
        "2019_Annual-Report_Haryana.csv"
        "2019_Annual-Report_Jammu-Kashmir.csv"
        "2019_Annual-Report_Karnataka.csv"
        "2019_Annual-Report_Kerala.csv"
        "2019_Annual-Report_Madhya-Pradesh.csv"
        "2019_Annual-Report_Maharashtra.csv"
        "2019_Annual-Report_Manipur.csv"
        "2019_Annual-Report_Mizoram.csv"
        "2019_Annual-Report_Odisha.csv"
        "2019_Annual-Report_Pondicherry.csv"
        "2019_Annual-Report_Punjab.csv"
        "2019_Annual-Report_Rajasthan.csv"
        "2019_Annual-Report_Tamil-Nadu.csv"
        "2019_Annual-Report_Telangana.csv"
        "2019_Annual-Report_Uttar-Pradesh.csv"
        "2019_Annual-Report_Uttarakhand.csv"
        "2019_Annual-Report_West-Bengal.csv"
    ]
    all_cleaning_steps(
        file,
        icar_inputs_dir();
        skiptotals = true,
        check_duplicated_states_ll = :Warn,
        check_allowed_serotypes_ll = :Warn,
        check_seroprevalence_as_pct_ll = :Error,
        check_aggregated_pre_post_counts_exist_ll = :Warn,
        check_pre_post_exists_ll = :Error,
        has_totals_ll = :Warn,
        calculate_all_totals_ll = :Warn,
        totals_check_state_ll = :Error,
        check_calculated_values_match_existing_ll = :Error,
        select_calculated_totals_ll = :Error,
        select_calculated_cols_ll = :Error,
        sort_columns_ll = :Error,
        sort_states_ll = :Error,
        write_csv_ll = :Error,
    )
end


#%%
all_cleaning_steps(
    "2019_Annual-Report_Haryana.csv",
    icar_inputs_dir();
    skiptotals = true,
    check_duplicated_states_ll = :Warn,
    check_allowed_serotypes_ll = :Warn,
    check_seroprevalence_as_pct_ll = :Error,
    check_aggregated_pre_post_counts_exist_ll = :Warn,
    check_pre_post_exists_ll = :Error,
    has_totals_ll = :Warn,
    calculate_all_totals_ll = :Warn,
    totals_check_state_ll = :Error,
    check_calculated_values_match_existing_ll = :Error,
    select_calculated_totals_ll = :Error,
    select_calculated_cols_ll = :Error,
    sort_columns_ll = :Error,
    sort_states_ll = :Error,
    write_csv_ll = :Error,
)

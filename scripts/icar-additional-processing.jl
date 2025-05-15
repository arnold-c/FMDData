#%%
using DrWatson
@quickactivate "FMDData"

using FMDData
using Try
using OrderedCollections: OrderedDict
using Logging
using LoggingExtras

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
icar_processed_logdir = icar_processed_dir("logfiles")
isdir(icar_processed_logdir) || mkpath(icar_processed_logdir)
logfile = joinpath(icar_processed_logdir, "nadcp_2.log")
logger = FileLogger(logfile)

with_logger(logger) do
    cumulative_nadcp_2_2022 = @? load_csv(
        "clean_2022_Annual-Report_NADCP-2.csv",
        icar_cleaned_dir()
    )

    nadcp_2_2021 = FMDData._log_try_error(
        load_csv(
            "clean_2021_Annual-Report_NADCP-2.csv",
            icar_cleaned_dir()
        )
    )

    nadcp_2_2022 = FMDData._log_try_error(
        infer_later_year_values(
            cumulative_nadcp_2_2022,
            nadcp_2_2021,
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            cumulative_nadcp_2_2022 => OrderedDict(
                :sample_year => "Combined",
                :report_year => 2022,
                :round_name => "NADCP 2",
                :test_type => "SPCE",
                :test_threshold => "1.65 log10 @ 35% inhibition"
            )
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            nadcp_2_2022 => OrderedDict(
                :sample_year => 2022,
                :report_year => 2022,
                :round_name => "NADCP 2",
                :test_type => "SPCE",
                :test_threshold => "1.65 log10 @ 35% inhibition"
            )
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            nadcp_2_2021 => OrderedDict(
                :sample_year => 2021,
                :report_year => 2021,
                :round_name => "NADCP 2",
                :test_type => "SPCE",
                :test_threshold => "1.65 log10 @ 35% inhibition"
            )
        )
    )

    nadcp_2 = FMDData._log_try_error(
        combine_round_dfs(
            cumulative_nadcp_2_2022, nadcp_2_2022, nadcp_2_2021
        )
    )

    FMDData._log_try_error(write_csv("nadcp_2_2022.csv", icar_processed_dir(), nadcp_2_2022))
    FMDData._log_try_error(write_csv("nadcp_2_2021.csv", icar_processed_dir(), nadcp_2_2021))
    FMDData._log_try_error(write_csv("nadcp_2.csv", icar_processed_dir(), nadcp_2))
end

#%%
logfile = joinpath(icar_processed_logdir, "nadcp_1.log")
logger = FileLogger(logfile)

with_logger(logger) do

    cumulative_nadcp_1_2021 = FMDData._log_try_error(
        load_csv(
            "clean_2021_Annual-Report_NADCP-1.csv",
            icar_cleaned_dir()
        )
    )

    nadcp_1_2020 = FMDData._log_try_error(
        load_csv(
            "clean_2020_Annual-Report_NADCP-1.csv",
            icar_cleaned_dir()
        )
    )

    nadcp_1_2021 = FMDData._log_try_error(
        infer_later_year_values(
            cumulative_nadcp_1_2021,
            nadcp_1_2020,
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            cumulative_nadcp_2_2021 => OrderedDict(
                :sample_year => "Combined",
                :report_year => 2021,
                :round_name => "NADCP 1",
                :test_type => "SPCE",
                :test_threshold => "1.8 log10 @ 50% inhibition"
            )
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            nadcp_2_2021 => OrderedDict(
                :sample_year => 2021,
                :report_year => 2021,
                :round_name => "NADCP 1",
                :test_type => "SPCE",
                :test_threshold => "1.8 log10 @ 50% inhibition"
            )
        )
    )

    FMDData._log_try_error(
        add_all_metadata!(
            nadcp_2_2020 => OrderedDict(
                :sample_year => 2020,
                :report_year => 2020,
                :round_name => "NADCP 1",
                :test_type => "SPCE",
                :test_threshold => "1.8 log10 @ 50% inhibition"
            )
        )
    )

    nadcp_1 = FMDData._log_try_error(
        combine_round_dfs(
            cumulative_nadcp_1_2021, nadcp_1_2021, nadcp_1_2020
        )
    )

    FMDData._log_try_error(
        write_csv("nadcp_1_2021.csv", icar_processed_dir(), nadcp_1_2021)
    )
    FMDData._log_try_error(
        write_csv("nadcp_1_2020.csv", icar_processed_dir(), nadcp_1_2020)
    )
    FMDData._log_try_error(
        write_csv("nadcp_1.csv", icar_processed_dir(), nadcp_1)
    )
end

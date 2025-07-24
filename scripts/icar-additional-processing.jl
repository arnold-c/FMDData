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
"""
    report_specific_processing_files_dir(args...)

Returns the absolute path to the `report-specific-processing-files` directory within the processed data folder,
which contains individual processing files for specific ICAR reports that are included in the main
processing pipeline.
"""
report_specific_processing_files_dir(args...) = scriptsdir("report-specific-processing-files", args...)


icar_processed_logdir = icar_processed_dir("logfiles")
isdir(icar_processed_logdir) || mkpath(icar_processed_logdir)

logfile = joinpath(icar_processed_logdir, "nadcp_2.log")
logger = FileLogger(logfile)

cumulative_nadcp_2_2022 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2022_Annual-Report_NADCP-2.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_2_2021 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2021_Annual-Report_NADCP-2.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_2_2022 = Try.@? FMDData._log_try_error(
    infer_later_year_values(
        cumulative_nadcp_2_2022,
        nadcp_2_2021,
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        cumulative_nadcp_2_2022 => OrderedDict(
            :sample_year => "2021/2022",
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_2_2022 => OrderedDict(
            :sample_year => 2022,
            :report_year => 2022,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_2_2021 => OrderedDict(
            :sample_year => 2021,
            :report_year => 2021,
            :round_name => "NADCP 2",
            :test_type => "SPCE",
            :test_threshold => "1.65 log10 @ 35% inhibition"
        )
    );
    logger = logger
)

nadcp_2 = Try.@? FMDData._log_try_error(
    combine_round_dfs(
        cumulative_nadcp_2_2022, nadcp_2_2022, nadcp_2_2021
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("nadcp_2_2022.csv", icar_processed_dir(), nadcp_2_2022);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_2_2021.csv", icar_processed_dir(), nadcp_2_2021);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_2.csv", icar_processed_dir(), nadcp_2);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end


#%%
logfile = joinpath(icar_processed_logdir, "nadcp_1.log")
logger = FileLogger(logfile)

cumulative_nadcp_1_2021 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2021_Annual-Report_NADCP-1.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_1_2020 = Try.@? FMDData._log_try_error(
    load_csv(
        "clean_2020_Annual-Report_NADCP-1.csv",
        icar_cleaned_dir()
    );
    logger = logger
)

nadcp_1_2021 = Try.@? FMDData._log_try_error(
    infer_later_year_values(
        cumulative_nadcp_1_2021,
        nadcp_1_2020,
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        cumulative_nadcp_1_2021 => OrderedDict(
            :sample_year => "2020/2021",
            :report_year => 2021,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_1_2021 => OrderedDict(
            :sample_year => 2021,
            :report_year => 2021,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    add_all_metadata!(
        nadcp_1_2020 => OrderedDict(
            :sample_year => 2020,
            :report_year => 2020,
            :round_name => "NADCP 1",
            :test_type => "SPCE",
            :test_threshold => "1.8 log10 @ 50% inhibition"
        )
    );
    logger = logger
)

nadcp_1 = Try.@? FMDData._log_try_error(
    combine_round_dfs(
        cumulative_nadcp_1_2021, nadcp_1_2021, nadcp_1_2020
    );
    logger = logger
)

Try.@? FMDData._log_try_error(
    write_csv("nadcp_1_2021.csv", icar_processed_dir(), nadcp_1_2021);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_1_2020.csv", icar_processed_dir(), nadcp_1_2020);
    logger = logger
)
Try.@? FMDData._log_try_error(
    write_csv("nadcp_1.csv", icar_processed_dir(), nadcp_1);
    logger = logger
)

if filesize(logfile) == 0
    rm(logfile)
end


#%%
# Process remaining non-2019 files

# Not including 2020 organized farm as has duplicate states and farms within a single year
# that will need to be handled in a special way
# include(report_specific_processing_files_dir("process-2020-organized-farms.jl"))

include(report_specific_processing_files_dir("process-2021-organized-farms.jl"))
include(report_specific_processing_files_dir("process-2022-nadcp-3.jl"))
include(report_specific_processing_files_dir("process-2022-organized-farms.jl"))

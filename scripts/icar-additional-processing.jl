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

#%%
include(report_specific_processing_files_dir("process-nadcp-2.jl"))
include(report_specific_processing_files_dir("process-nadcp-1.jl"))

# Not including 2020 organized farm as has duplicate states and farms within a single year
# that will need to be handled in a special way
# include(report_specific_processing_files_dir("process-2020-organized-farms.jl"))

include(report_specific_processing_files_dir("process-2021-organized-farms.jl"))
include(report_specific_processing_files_dir("process-2022-nadcp-3.jl"))
include(report_specific_processing_files_dir("process-2022-organized-farms.jl"))

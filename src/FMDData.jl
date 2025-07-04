"""
    FMDData

A Julia package for cleaning and processing Foot-and-Mouth Disease (FMD) seroprevalence data from Indian Council of Agricultural Research (ICAR) annual reports.
"""
module FMDData

include("./consts.jl")
include("./utils.jl")
include("./error-handlers.jl")
include("./icar-cleaning/calculate-state-counts.jl")
include("./icar-cleaning/calculate-state-seroprevalence.jl")
include("./icar-cleaning/check-calculated-values.jl")
include("./icar-cleaning/check-seroprevalence-values.jl")
include("./icar-cleaning/clean-column-names.jl")
include("./icar-cleaning/column-name-checks.jl")
include("./icar-cleaning/file-management.jl")
include("./icar-cleaning/pre-post-checks.jl")
include("./icar-cleaning/select-calculated-columns.jl")
include("./icar-cleaning/serotype-checks.jl")
include("./icar-cleaning/sort-data.jl")
include("./icar-cleaning/state-checks.jl")
include("./icar-cleaning/state-keys.jl")
include("./icar-cleaning/total-row-functions.jl")
include("./icar-cleaning/wrapper-functions.jl")
include("./icar-processing/icar-processing-functions.jl")


using PrecompileTools: @setup_workload, @compile_workload
using DrWatson: srcdir
using Preferences: set_preferences!, delete_preferences!

# Don't run precompilation steps if in temporary test environment
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => false)
if isdir(icar_inputs_dir())

    @setup_workload begin
        @compile_workload begin
            all_cleaning_steps(
                "2022_Annual-Report_NADCP-2.csv",
                icar_inputs_dir();
                output_dir = srcdir()
            )

            all_cleaning_steps(
                "2021_Annual-Report_NADCP-2.csv",
                icar_inputs_dir();
                output_dir = srcdir()
            )

            cumulative_nadcp_2_2022 = FMDData._log_try_error(
                load_csv(
                    "clean_2022_Annual-Report_NADCP-2.csv",
                    srcdir()
                )
            )

            nadcp_2_2021 = FMDData._log_try_error(
                load_csv(
                    "clean_2021_Annual-Report_NADCP-2.csv",
                    srcdir()
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
        end
        # Clean up files from precompile steps
        dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(srcdir()))
        for file in dir_files
            rm(srcdir(file))
        end
        rm(srcdir("logfiles"); recursive = true)
    end
end

# Reset preferences to show warnings during package use
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => true)

# Include to help LSP work in files outside of the src/ dir
@static if false
    include("../scripts/icar-cleaning.jl")
    include("../scripts/icar-additional-processing.jl")
    include("../test/icar-cleaning-functions.jl")
end

end # module

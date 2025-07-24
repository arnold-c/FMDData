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
include("./icar-processing/metadata-operations.jl")
include("./icar-processing/data-inference.jl")
include("./icar-processing/dataframe-operations.jl")

using Preferences: set_preferences!, delete_preferences!

# Precompilation
# include("./precompilation.jl")

# reset warnings preferences after precompilation steps
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => true)


# Include to help LSP work in files outside of the src/ dir
@static if false
    include("../scripts/icar-cleaning.jl")
    include("../scripts/icar-additional-processing.jl")
    include("../test/utils.jl")
    include("../test/error-handlers.jl")
    include("../test/icar-cleaning/calculate-state-counts.jl")
    include("../test/icar-cleaning/calculate-state-seroprevalence.jl")
    include("../test/icar-cleaning/check-calculated-values.jl")
    include("../test/icar-cleaning/check-seroprevalence-values.jl")
    include("../test/icar-cleaning/clean-column-names.jl")
    include("../test/icar-cleaning/column-name-checks.jl")
    include("../test/icar-cleaning/file-management.jl")
    include("../test/icar-cleaning/pre-post-checks.jl")
    include("../test/icar-cleaning/select-calculated-columns.jl")
    include("../test/icar-cleaning/serotype-checks.jl")
    include("../test/icar-cleaning/sort-data.jl")
    include("../test/icar-cleaning/state-checks.jl")
    include("../test/icar-cleaning/state-keys.jl")
    include("../test/icar-cleaning/total-row-functions.jl")
    include("../test/icar-cleaning/wrapper-functions.jl")
    include("../test/icar-processing/metadata-operations.jl")
    include("../test/icar-processing/data-inference.jl")
    include("../test/icar-processing/dataframe-operations.jl")
end

end # module

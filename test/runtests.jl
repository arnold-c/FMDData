using FMDData
using Test
using JET
import Aqua
using PrecompileTools
using Preferences
using DrWatson: findproject

test_dir(args...) = joinpath(findproject(), "test", args...)

# Turn off warnings during testing
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => false)

@testset verbose = true "FMDData" begin
    # @testset "Static analysis with JET.jl" begin
    #     @test isempty(JET.get_reports(report_package(FMDData, target_modules = (FMDData,))))
    # end
    #
    # @testset "QA with Aqua" begin
    #     Aqua.test_all(FMDData)
    # end

    include("./utils.jl")
    include("./error-handlers.jl")
    # include("./icar-cleaning/aggregate-count-checks.jl")
    # include("./icar-cleaning/calculate-state-counts.jl")
    # include("./icar-cleaning/calculate-state-seroprevalence.jl")
    # include("./icar-cleaning/check-calculated-values.jl")
    # include("./icar-cleaning/check-seroprevalence-values.jl")
    # include("./icar-cleaning/clean-column-names.jl")
    # include("./icar-cleaning/column-name-checks.jl")
    # include("./icar-cleaning/file-management.jl")
    # include("./icar-cleaning/pre-post-checks.jl")
    # include("./icar-cleaning/select-calculated-columns.jl")
    # include("./icar-cleaning/serotype-checks.jl")
    # include("./icar-cleaning/sort-data.jl")
    # include("./icar-cleaning/state-checks.jl")
    # include("./icar-cleaning/state-keys.jl")
    # include("./icar-cleaning/total-row-functions.jl")
    # include("./icar-cleaning/wrapper-functions.jl")
    include("./icar-cleaning/integration-test.jl")

    # include("./icar-processing/icar-processing-functions.jl")
end
#
# Reset preferences to show warnings during package use
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => true)

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

    include("./icar-cleaning-functions.jl")
    # include("./icar-processing-functions.jl")
    include("./error-handlers.jl")
end
#
# Reset preferences to show warnings during package use
delete_preferences!(FMDData, "show_warnings"; force = true)
set_preferences!(FMDData, "show_warnings" => true)

using FMDData
using Test
using JET
import Aqua

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
end

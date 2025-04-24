using FMDData
using Test
using JET
import Aqua

@testset "FMDData" begin
    @testset "Static analysis with JET.jl" begin
        @test isempty(JET.get_reports(report_package(FMDData, target_modules = (FMDData,))))
    end

    @testset "QA with Aqua" begin
        Aqua.test_all(FMDData)
    end

    # include("../test/icar-cleaning-functions.jl")
end

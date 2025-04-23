using FMDData
using Test

using JET
@testset "static analysis with JET.jl" begin
    @test isempty(JET.get_reports(report_package(FMDData, target_modules=(FMDData,))))
end

@testset "QA with Aqua" begin
    import Aqua
    Aqua.test_all(FMDData)
end

# write tests here



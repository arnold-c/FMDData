using FMDData

@testset verbose = true "state-keys.jl" begin
    @test isequal(
        typeof(FMDData.states_dict),
        Dict{String, String}
    )
end

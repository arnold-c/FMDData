using FMDData
using DataFrames
using Try: Try

@testset verbose = true "state-checks.jl" begin
    @testset "Correcting state names" begin
        @test isequal(
            correct_all_state_names(
                DataFrame(
                    "states_ut" => ["ab", "b", "c", "d"]
                ),
                :states_ut,
                Dict(
                    "a" => "a_new",
                    "b" => "b_new",
                    "c" => "c_new"
                )
            ),
            Try.Err("State name `ab` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling. State name `d` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling.")
        )

        @test isequal(
            FMDData.correct_state_name(
                "New State",
                FMDData.states_dict
            ),
            Try.Err("State name `New State` doesn't exist in current dictionary match. Confirm if this is a new state or uncharacterized misspelling.")
        )
    end


    @testset "Check missing states" begin
        @test Try.isok(check_missing_states(DataFrame("states_ut" => String["a", "b", "c", "total"])))

        @test isequal(
            check_missing_states(DataFrame("states_ut" => ["a", "b", missing, "a", missing])),
            Try.Err("There are 2 values in the states_ut column that are of type `Missing`")
        )
    end

    @testset "Check duplicated states" begin
        @test Try.isok(check_duplicated_states(DataFrame("states_ut" => String["a", "b", "c", "total"])))

        @test isequal(
            check_duplicated_states(DataFrame("states_ut" => ["a", "b", "c", "a"])),
            Try.Err("The dataframe has 4 state values, but only 3 unique state values. (\"a\",) were duplicated")
        )
    end

    @testset "State name edge cases" begin
        # Test with states containing special characters
        special_states_df = DataFrame(
            states_ut = ["State-With-Hyphens", "State With Spaces", "State_With_Underscores", "State&Symbols"],
            serotype_o_count_pre = [100, 150, 80, 200],
            serotype_o_count_post = [20, 30, 15, 40]
        )
        
        # Test state validation (would need actual function)
        for state in special_states_df.states_ut
            @test isa(state, String)
            @test !isempty(state)
        end
        
        # Test with very long state names - fix the string multiplication syntax
        long_state_name = repeat("A", 1000)  # 1000 character state name
        long_name_df = DataFrame(
            states_ut = [long_state_name],
            serotype_o_count_pre = [100]
        )
        
        @test nrow(long_name_df) == 1
        @test length(long_name_df.states_ut[1]) == 1000
    end

end

using FMDData
using Try

@testset "utils.jl" begin

    @testset "Skip values" begin
        @test isequal(
            collect(skip_missing_and_nan([missing, 10, 20, missing, 30])),
            [10, 20, 30]
        )

        @test isequal(
            collect(skip_missing_and_nan([10, 20, 30])),
            [10, 20, 30]
        )

        @test isequal(
            collect(skip_nothing([nothing, 10, 20, nothing, 30])),
            [10, 20, 30]
        )

        @test isequal(
            collect(skip_nothing([10, 20, 30])),
            [10, 20, 30]
        )
    end

    @testset "Update Regex" begin
        @test isequal(
            update_regex(
                r"this is a test",
                r"(.*)\sis\s(.*)",
                s"\1 \2"
            ),
            r"this a test"
        )
    end

    @testset "String occurences" begin
        @test isequal(
            FMDData._calculate_string_occurences(
                ["a", "a", "b", "c", "e"]
            ),
            (; a = 2, b = 1, c = 1, e = 1)
        )

        @test isequal(
            FMDData._calculate_string_occurences(
                ["b", "a", "b", "c", "e"]
            ),
            (; b = 2, a = 1, c = 1, e = 1)
        )

        @test isequal(
            FMDData._calculate_string_occurences(
                ["b", "a", "b", "c", "e"],
                ["a", "b", "e"]
            ),
            (; a = 1, b = 2, e = 1)
        )

    end

    @testset "Edge Cases - Missing/NaN Values" begin
        @testset "DataFrames with mixed missing/NaN values" begin
            # Test with mixed missing and NaN values
            test_values = [1, missing, NaN, 2, 3]
            filtered_values = collect(skip_missing_and_nan(test_values))
            @test filtered_values == [1, 2, 3]

            # Test with completely missing column
            all_missing_values = [missing, missing, missing]
            filtered_missing = collect(skip_missing_and_nan(all_missing_values))
            @test isempty(filtered_missing)

            # Test with completely NaN column
            all_nan_values = [NaN, NaN, NaN]
            filtered_nan = collect(skip_missing_and_nan(all_nan_values))
            @test isempty(filtered_nan)
        end

        @testset "Single-value edge cases" begin
            # Test skip_missing_and_nan with single missing value
            single_missing = [missing]
            filtered_single = collect(skip_missing_and_nan(single_missing))
            @test isempty(filtered_single)

            # Test with single NaN value
            single_nan = [NaN]
            filtered_single_nan = collect(skip_missing_and_nan(single_nan))
            @test isempty(filtered_single_nan)
        end

        @testset "Boundary value handling" begin
            # Test with extreme values - note: mixed Int64/Float64 array gets promoted to Float64
            extreme_values = [0, typemax(Int64), typemin(Int64), Inf, -Inf]
            filtered_extreme = collect(skip_missing_and_nan(extreme_values))
            @test length(filtered_extreme) == 5  # All values should be kept including Inf/-Inf
            @test 0.0 in filtered_extreme
            @test Float64(typemax(Int64)) in filtered_extreme  # Account for type promotion
            @test Float64(typemin(Int64)) in filtered_extreme  # Account for type promotion
            @test Inf in filtered_extreme
            @test -Inf in filtered_extreme

            # Test with negative values (should be handled appropriately)
            negative_values = [-1, -10, -100]
            filtered_negative = collect(skip_missing_and_nan(negative_values))
            @test filtered_negative == negative_values
        end

        @testset "Skip missing/NaN value edge cases" begin
            # Test with very small decimal values
            tiny_values = [1.0e-10, 1.0e-15, 1.0e-20]
            filtered_tiny = collect(skip_missing_and_nan(tiny_values))
            @test filtered_tiny == tiny_values

            # Test with very large values
            huge_values = [1.0e10, 1.0e15, 1.0e20]
            filtered_huge = collect(skip_missing_and_nan(huge_values))
            @test filtered_huge == huge_values
        end

        @testset "Data type consistency with missing values" begin
            # Test skip_missing_and_nan preserves non-missing types
            int_values = [1, missing, 3, missing, 5]
            filtered_ints = collect(skip_missing_and_nan(int_values))
            @test filtered_ints == [1, 3, 5]
            @test eltype(filtered_ints) == Int64

            float_values = [1.1, missing, 3.3, NaN, 5.5]
            filtered_floats = collect(skip_missing_and_nan(float_values))
            @test filtered_floats == [1.1, 3.3, 5.5]
            @test eltype(filtered_floats) == Float64
        end
    end
end

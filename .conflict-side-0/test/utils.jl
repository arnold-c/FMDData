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
end

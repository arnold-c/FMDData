using FMDData
using Try: Try
using DrWatson: findproject
using DataFrames

@testset verbose = true "file-management.jl" begin
    test_dir(args...) = joinpath(findproject(), args...)

    @testset "Load CSV" begin

        filename = "missing-data.csv"
        not_a_dir = "./not-a-dir.txt"


        @test isequal(
            load_csv("missing-data.tsv", not_a_dir),
            Try.Err("$not_a_dir is not a valid directory")
        )

        dir = test_dir()

        @test isequal(
            load_csv(filename, dir),
            Try.Err("$filename is not within the directory $dir")
        )

        filename = "test-data.csv"

        data = Try.@? load_csv(
            filename,
            dir,
            DataFrame
        )

        @test isequal(
            typeof(data),
            DataFrame
        )

        expected_col_names = [
            "States/UT",
            "Pre (N)",
            "Post (N)",
            "Serotype O (%) Pre",
            "Serotype O (%) Post",
            "Serotype A (%) Pre",
            "Serotype A (%) Post",
            "Serotype Asia1 (%) Pre",
            "Serotype Asia1 (%) Post",
        ]

        expected_col_types = Dict(
            zip(
                expected_col_names, [
                    AbstractString,
                    Union{Missing, Integer},
                    Union{Missing, Integer},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                    Union{Missing, AbstractFloat},
                ]
            )
        )

        @test isequal(
            names(data),
            expected_col_names
        )

        for (j, col) in pairs(eachcol(data))
            @test eltype(col) <: expected_col_types[String(j)]
        end
    end

    @testset "Write CSV" begin
        @test isequal(
            write_csv(
                "test",
                test_dir(),
                DataFrame("a" => String[])
            ),
            Try.Err("test is not a csv file")
        )
    end
end

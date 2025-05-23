using FMDData
using Try

@testset "error-handlers.jl" begin
    @testset "Logging errors" begin
        @test isequal(
            FMDData._log_try_error(Try.Err("This is a warning"), :Error),
            Try.Err("This is a warning")

        )

        @test isequal(
            FMDData._log_try_error(Try.Ok("This works")),
            "This works"
        )

        @test isequal(
            FMDData._log_try_error(Try.Ok(10); unwrap_ok = false),
            Try.Ok(10)
        )
    end

    @testset "Combining error message" begin
        @test isequal(
            FMDData._combine_error_messages(
                [
                    Try.Ok("This should be filtered out."),
                    Try.Err("Include this."),
                    Try.Err("... and this."),
                ];
                filter_ok = true
            ),
            "Include this. ... and this."
        )

        @test isequal(
            FMDData._combine_error_messages(
                [
                    Try.Ok("This should be no longer be filtered out."),
                    Try.Err("Include this."),
                    Try.Err("... and this."),
                ];
                filter_ok = false
            ),
            "This should be no longer be filtered out. Include this. ... and this."
        )
    end


    @testset "Unwrapping errors" begin
        @test isequal(
            FMDData._unwrap_err_or_empty_str(Try.Err("Error message")),
            "Error message"
        )

        @test isequal(
            FMDData._unwrap_err_or_empty_str(Try.Ok("Ok message")),
            "Ok message"
        )

        @test isequal(
            FMDData._unwrap_err_or_empty_str(Try.Ok(nothing)),
            ""
        )
    end

end

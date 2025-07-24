using PrecompileTools: @setup_workload, @compile_workload
using DrWatson: srcdir

# Don't run precompilation steps if in temporary test environment
if isdir(icar_inputs_dir())

    @setup_workload begin
        @compile_workload begin
            all_cleaning_steps(
                "2022_Annual-Report_NADCP-2.csv",
                icar_inputs_dir();
                output_dir = srcdir()
            )

            all_cleaning_steps(
                "2021_Annual-Report_NADCP-2.csv",
                icar_inputs_dir();
                output_dir = srcdir()
            )

            cumulative_nadcp_2_2022 = FMDData._log_try_error(
                load_csv(
                    "clean_2022_Annual-Report_NADCP-2.csv",
                    srcdir()
                )
            )

            nadcp_2_2021 = FMDData._log_try_error(
                load_csv(
                    "clean_2021_Annual-Report_NADCP-2.csv",
                    srcdir()
                )
            )

            nadcp_2_2022 = FMDData._log_try_error(
                infer_later_year_values(
                    cumulative_nadcp_2_2022,
                    nadcp_2_2021,
                )
            )

            FMDData._log_try_error(
                add_all_metadata!(
                    cumulative_nadcp_2_2022 => OrderedDict(
                        :sample_year => "Combined",
                        :report_year => 2022,
                        :round_name => "NADCP 2",
                        :test_type => "SPCE",
                        :test_threshold => "1.65 log10 @ 35% inhibition"
                    )
                )
            )
        end
        # Clean up files from precompile steps
        dir_files = filter(t -> contains(t, r".*\.csv$"), readdir(srcdir()))
        for file in dir_files
            rm(srcdir(file))
        end
        rm(srcdir("logfiles"); recursive = true)
    end
end

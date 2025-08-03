using DataFrames: DataFrame
using Try: Try
using LoggingExtras: FileLogger

export all_cleaning_steps

show_warnings = @load_preference("show_warnings", true)

"""
    all_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
        output_filename::T1 = "clean_\$(input_filename)",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame,
        skiptotals = false,
        [logging_level_parameters...]
    ) where {T1 <: AbstractString}

A wrapper function that runs all the cleaning steps for seroprevalence tables. This function handles all data formats including tables with multiple rows per state (e.g., 2019 report tables) through the `skiptotals` parameter and conditional logic.

# Arguments
- `input_filename`: Name of the CSV file to process
- `input_dir`: Directory containing the input file
- `output_filename`: Name for the cleaned output file (defaults to "clean_" + input filename)
- `output_dir`: Directory for output (defaults to cleaned data directory)
- `load_format`: Format for loading data (defaults to DataFrame)
- `skiptotals`: If true, skips totals-related processing for datasets without totals rows
- `*_ll`: Logging level parameters for each processing step (:Error, :Warn, or :Info)

# Returns
- `Try.Ok(nothing)` on success
- `Try.Err(message)` if any critical errors occur

# Examples
```julia
# Standard processing
all_cleaning_steps("2022_NADCP-2.csv", icar_inputs_dir())

# Skip totals for 2019 data
all_cleaning_steps("2019_Bihar.csv", icar_inputs_dir(); skiptotals = true)
```
"""
function all_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
        output_filename::T1 = "clean_$input_filename",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame,
        skiptotals = false,
        load_csv_ll = :Error,
        clean_colnames_ll = :Error,
        rename_aggregated_pre_post_counts_ll = :Error,
        correct_all_state_names_ll = :Error,
        add_state_code_ll = :Error,
        check_duplicated_column_names_ll = :Error,
        check_missing_states_ll = :Error,
        check_duplicated_states_ll = :Error,
        check_allowed_serotypes_ll = :Error,
        check_seroprevalence_as_pct_ll = :Error,
        check_aggregated_pre_post_counts_exist_ll = :Error,
        check_pre_post_exists_ll = :Error,
        calculate_all_totals_ll = :Error,
        has_totals_ll = :Error,
        calculated_totals_match_ll = :Warn,
        check_calculated_values_match_existing_ll = :Warn,
        select_calculated_totals_ll = :Warn,
        select_calculated_cols_ll = :Warn,
        sort_columns_ll = :Warn,
        sort_states_ll = :Warn,
        write_csv_ll = :Error,
    ) where {T1 <: AbstractString}

    if !isdir(output_dir)
        mkpath(output_dir)
    end
    logpath = joinpath(output_dir, "logfiles")
    if !isdir(logpath)
        mkpath(logpath)
    end

    filebase = match(r"(.*)\.csv", input_filename).captures[1]
    logfile = joinpath(output_dir, "logfiles", "$filebase.log")
    logger = FileLogger(logfile)

    data = Try.@? _log_try_error(
        load_csv(
            input_filename,
            input_dir,
            load_format
        ),
        load_csv_ll;
        logger = logger
    )

    cleaned_colnames_data = Try.@? _log_try_error(
        clean_colnames(data),
        clean_colnames_ll;
        logger = logger
    )

    renamed_aggregate_counts_data = Try.@? _log_try_error(
        rename_aggregated_pre_post_counts(cleaned_colnames_data),
        rename_aggregated_pre_post_counts_ll;
        logger = logger
    )

    corrected_state_name_data = Try.@? _log_try_error(
        correct_all_state_names(renamed_aggregate_counts_data),
        correct_all_state_names_ll;
        logger = logger
    )

    Try.@? _log_try_error(
        check_duplicated_column_names(corrected_state_name_data),
        check_duplicated_column_names_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        check_missing_states(corrected_state_name_data),
        check_missing_states_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        check_duplicated_states(corrected_state_name_data),
        check_duplicated_states_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        check_allowed_serotypes(corrected_state_name_data),
        check_allowed_serotypes_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        check_seroprevalence_as_pct(corrected_state_name_data),
        check_seroprevalence_as_pct_ll;
        logger = logger
    )

    aggregate_pre_post_exist_result = check_aggregated_pre_post_counts_exist(corrected_state_name_data)
    Try.@? _log_try_error(
        aggregate_pre_post_exist_result,
        check_aggregated_pre_post_counts_exist_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        check_pre_post_exists(corrected_state_name_data),
        check_pre_post_exists_ll;
        logger = logger
    )

    has_totals = has_totals_row(corrected_state_name_data)

    Try.@? _log_try_error(
        has_totals, has_totals_ll; logger = logger
    )

    if skiptotals
        calculated_totals_dict = Try.Err(nothing)
    else
        calculated_totals_dict = calculate_all_totals(corrected_state_name_data)

        Try.@? _log_try_error(
            calculated_totals_dict,
            calculate_all_totals_ll;
            logger = logger
        )
    end

    cleaned_data = corrected_state_name_data

    # If totals can be calculated, then calculate values that rely on the totals
    if Try.isok(calculated_totals_dict)
        calculated_totals_dict = Try.unwrap(calculated_totals_dict)
        # Check if calculated and provided totals match
        calculated_totals_match = all_totals_check(calculated_totals_dict, cleaned_data)

        # If a totals row doesn't exist then use calculated totals values
        if Try.iserr(has_totals)
            Try.@? _log_try_error(
                has_totals,
                has_totals_ll;
                logger = logger
            )
            push!(
                cleaned_data,
                merge(Dict("states_ut" => "Total calculated"), calculated_totals_dict);
                promote = true
            )
            # If calculated and provided totals don't match then add calculated totals
        elseif Try.iserr(calculated_totals_match)
            Try.@? _log_try_error(
                calculated_totals_match,
                calculated_totals_match_ll;
                logger = logger
            )
            push!(
                cleaned_data,
                merge(Dict("states_ut" => "Total calculated"), calculated_totals_dict);
                promote = true
            )
        end

        Try.@? _log_try_error(
            select_calculated_totals!(cleaned_data),
            select_calculated_totals_ll;
            logger = logger
        )
    end

    if Try.isok(aggregate_pre_post_exist_result)
        # Calculate missing state count and seroprevalence values that may be missing
        cleaned_data = calculate_state_counts(cleaned_data)
        cleaned_data = calculate_state_seroprevalence(cleaned_data)

        Try.@? _log_try_error(
            check_calculated_values_match_existing(cleaned_data),
            check_calculated_values_match_existing_ll;
            logger = logger
        )
        Try.@? _log_try_error(
            select_calculated_cols!(cleaned_data),
            select_calculated_cols_ll;
            logger = logger
        )

    end

    Try.@? _log_try_error(
        add_state_code!(cleaned_data),
        add_state_code_ll;
        logger = logger
    )

    Try.@? _log_try_error(
        sort_columns!(cleaned_data),
        sort_columns_ll;
        logger = logger
    )
    Try.@? _log_try_error(
        sort_states!(cleaned_data),
        sort_states_ll;
        logger = logger
    )

    Try.@? _log_try_error(
        write_csv(output_filename, output_dir, cleaned_data),
        write_csv_ll;
        logger = logger
    )

    if filesize(logfile) == 0
        rm(logfile)
    end

    return Try.Ok(
        (
            "Cleaning of $input_filename successful. Written to $output_filename.",
            cleaned_data,
        )
    )
end
#
# """
#     all_2019_cleaning_steps(
#         input_filename::T1,
#         input_dir::T1;
# 		output_filename::T1 = "clean_\$(input_filename)",
#         output_dir::T1 = icar_cleaned_dir(),
#         load_format = DataFrame
#     ) where {T1 <: AbstractString}
#
#
# A wrapper function that runs all the cleaning steps for seroprevalence tables from the 2019 annual report that share the common format of states in each row and columns relating to serotype seroprevalence. For tables from later reports, use [`all_cleaning_steps()`](@ref)
# """
# function all_2019_cleaning_steps(
#         input_filename::T1,
#         input_dir::T1;
#         output_filename::T1 = "clean_$input_filename",
#         output_dir::T1 = icar_cleaned_dir(),
#         load_format = DataFrame
#     ) where {T1 <: AbstractString}
#
#     if show_warnings
#         println("\n==========================================================================")
#         println("Cleaning $(joinpath(input_dir, input_filename))\n")
#     end
#
#     if !isdir(output_dir)
#         mkpath(output_dir)
#     end
#     logpath = joinpath(output_dir, "logfiles")
#     if !isdir(logpath)
#         mkpath(logpath)
#     end
#
#     filebase = match(r"(.*)\.csv", input_filename).captures[1]
#     logfile = joinpath(output_dir, "logfiles", "$filebase.log")
#     logger = FileLogger(logfile)
#
#     with_logger(logger) do
#         data = _log_try_error(
#             load_csv(
#                 input_filename,
#                 input_dir,
#                 load_format
#             )
#         )
#         cleaned_colnames_data = _log_try_error(clean_colnames(data))
#         renamed_aggregate_counts_data = _log_try_error(
#             rename_aggregated_pre_post_counts(cleaned_colnames_data)
#         )
#         corrected_state_name_data = _log_try_error(
#             correct_all_state_names(renamed_aggregate_counts_data)
#         )
#
#         _log_try_error(check_duplicated_column_names(corrected_state_name_data))
#         _log_try_error(check_missing_states(corrected_state_name_data))
#         _log_try_error(check_allowed_serotypes(corrected_state_name_data), :Warn)
#         _log_try_error(check_seroprevalence_as_pct(corrected_state_name_data))
#         aggregated_counts_exist = check_aggregated_pre_post_counts_exist(corrected_state_name_data)
#         _log_try_error(aggregated_counts_exist, :Warn)
#         _log_try_error(check_pre_post_exists(corrected_state_name_data))
#
#         has_totals = has_totals_row(corrected_state_name_data)
#         if Try.isok(has_totals)
#             _log_try_error(Try.Err("Found a totals row when one shouldn't exist"))
#         end
#
#         out_df = corrected_state_name_data
#         if Try.isok(aggregated_counts_exist)
#             out_df = calculate_state_counts(out_df)
#             out_df = calculate_state_seroprevalence(out_df)
#         end
#
#         _log_try_error(
#             check_calculated_values_match_existing(out_df)
#         )
#
#         if Try.isok(aggregated_counts_exist)
#             _log_try_error(select_calculated_cols!(out_df))
#         end
#
#         _log_try_error(sort_columns!(out_df))
#         _log_try_error(sort_states!(out_df))
#
#         _log_try_error(
#             write_csv(output_filename, output_dir, out_df)
#         )
#     end
#
#     if filesize(logfile) == 0
#         rm(logfile)
#     end
#
#     return Try.Ok("Cleaning of $input_filename successful. Written to $output_filename.")
# end

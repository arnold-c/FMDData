using DataFrames: DataFrame
using Try: Try
using Logging: with_logger
using LoggingExtras: FileLogger

export all_cleaning_steps,
    all_2019_cleaning_steps

show_warnings = @load_preference("show_warnings", true)

"""
    all_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
		output_filename::T1 = "clean_\$(input_filename)",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame
    ) where {T1 <: AbstractString}

A wrapper function that runs all the cleaning steps for seroprevalence tables that share the common format of states in each row and columns relating to serotype counts/seroprevalence. For tables that contain multiple rows for each state e.g., 2019 report tables which cover multiple years for a single state, use the relevant alternative wrapper functions [`all_2019_cleaning_steps()`](@ref).
"""
function all_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
        output_filename::T1 = "clean_$input_filename",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame
    ) where {T1 <: AbstractString}

    if show_warnings
        println("\n==========================================================================")
        println("Cleaning $(joinpath(input_dir, input_filename))\n")
    end

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

    with_logger(logger) do
        data = _log_try_error(
            load_csv(
                input_filename,
                input_dir,
                load_format
            )
        )
        cleaned_colnames_data = _log_try_error(clean_colnames(data))
        renamed_aggregate_counts_data = _log_try_error(
            rename_aggregated_pre_post_counts(cleaned_colnames_data)
        )
        corrected_state_name_data = _log_try_error(
            correct_all_state_names(renamed_aggregate_counts_data)
        )

        _log_try_error(check_duplicated_column_names(corrected_state_name_data))
        _log_try_error(check_missing_states(corrected_state_name_data))
        _log_try_error(check_duplicated_states(corrected_state_name_data))
        _log_try_error(check_allowed_serotypes(corrected_state_name_data))
        _log_try_error(check_seroprevalence_as_pct(corrected_state_name_data))
        _log_try_error(check_aggregated_pre_post_counts_exist(corrected_state_name_data))
        _log_try_error(check_pre_post_exists(corrected_state_name_data))

        has_totals = has_totals_row(corrected_state_name_data)
        totals_dict = _log_try_error(calculate_all_totals(corrected_state_name_data))
        totals_check_state = all_totals_check(totals_dict, corrected_state_name_data)
        if Try.iserr(has_totals)
            _log_try_error(has_totals, :Warn)
            push!(
                corrected_state_name_data,
                merge(Dict("states_ut" => "Total calculated"), Try.unwrap_err(totals_dict));
                promote = true
            )
        elseif Try.iserr(totals_check_state)
            _log_try_error(
                totals_check_state,
                :Warn
            )
            push!(
                corrected_state_name_data,
                merge(Dict("states_ut" => "Total calculated"), totals_dict);
                promote = true
            )
        end
        calculated_state_counts_data = calculate_state_counts(corrected_state_name_data)
        calculated_state_seroprevs_data = calculate_state_seroprevalence(calculated_state_counts_data)

        _log_try_error(
            check_calculated_values_match_existing(calculated_state_seroprevs_data)
        )

        _log_try_error(select_calculated_totals!(calculated_state_seroprevs_data))
        _log_try_error(select_calculated_cols!(calculated_state_seroprevs_data))

        _log_try_error(sort_columns!(calculated_state_seroprevs_data))
        _log_try_error(sort_states!(calculated_state_seroprevs_data))

        _log_try_error(
            write_csv(output_filename, output_dir, calculated_state_seroprevs_data)
        )
    end

    if filesize(logfile) == 0
        rm(logfile)
    end

    return Try.Ok("Cleaning of $input_filename successful. Written to $output_filename.")
end

"""
    all_2019_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
		output_filename::T1 = "clean_\$(input_filename)",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame
    ) where {T1 <: AbstractString}


A wrapper function that runs all the cleaning steps for seroprevalence tables from the 2019 annual report that share the common format of states in each row and columns relating to serotype seroprevalence. For tables from later reports, use [`all_cleaning_steps()`](@ref)
"""
function all_2019_cleaning_steps(
        input_filename::T1,
        input_dir::T1;
        output_filename::T1 = "clean_$input_filename",
        output_dir::T1 = icar_cleaned_dir(),
        load_format = DataFrame
    ) where {T1 <: AbstractString}

    if show_warnings
        println("\n==========================================================================")
        println("Cleaning $(joinpath(input_dir, input_filename))\n")
    end

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

    with_logger(logger) do
        data = _log_try_error(
            load_csv(
                input_filename,
                input_dir,
                load_format
            )
        )
        cleaned_colnames_data = _log_try_error(clean_colnames(data))
        renamed_aggregate_counts_data = _log_try_error(
            rename_aggregated_pre_post_counts(cleaned_colnames_data)
        )
        corrected_state_name_data = _log_try_error(
            correct_all_state_names(renamed_aggregate_counts_data)
        )

        _log_try_error(check_duplicated_column_names(corrected_state_name_data))
        _log_try_error(check_missing_states(corrected_state_name_data))
        _log_try_error(check_allowed_serotypes(corrected_state_name_data), :Warn)
        _log_try_error(check_seroprevalence_as_pct(corrected_state_name_data))
        aggregated_counts_exist = check_aggregated_pre_post_counts_exist(corrected_state_name_data)
        _log_try_error(aggregated_counts_exist, :Warn)
        _log_try_error(check_pre_post_exists(corrected_state_name_data))

        has_totals = has_totals_row(corrected_state_name_data)
        if Try.isok(has_totals)
            _log_try_error(Try.Err("Found a totals row when one shouldn't exist"))
        end

        out_df = corrected_state_name_data
        if Try.isok(aggregated_counts_exist)
            out_df = calculate_state_counts(out_df)
            out_df = calculate_state_seroprevalence(out_df)
        end

        _log_try_error(
            check_calculated_values_match_existing(out_df)
        )

        if Try.isok(aggregated_counts_exist)
            _log_try_error(select_calculated_cols!(out_df))
        end

        _log_try_error(sort_columns!(out_df))
        _log_try_error(sort_states!(out_df))

        _log_try_error(
            write_csv(output_filename, output_dir, out_df)
        )
    end

    if filesize(logfile) == 0
        rm(logfile)
    end

    return Try.Ok("Cleaning of $input_filename successful. Written to $output_filename.")
end

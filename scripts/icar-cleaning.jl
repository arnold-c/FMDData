#%%
using DrWatson
@quickactivate "FMDData"

using FMDData
using CSV
using DataFrames

#%%
nadcp_2 = load_csv(
    "2022_Annual-Report_NADCP-2.csv",
    icar_inputs_dir()
) |>
    clean_colnames

#%%
has_totals_row(nadcp_2)

correct_all_state_names(nadcp_2)

check_duplicated_states(nadcp_2)

aggregated_nadcp_2 = rename_aggregated_pre_post_counts(nadcp_2)

#%%
all_totals_check(aggregated_nadcp_2)

#%%
calculated_count_data = calculate_state_counts(aggregated_nadcp_2)

calculated_count_no_seroprev_data = rename(
    s -> replace(s, r"(.*)_calculated$" => s"\1"),
    select(calculated_count_data, Not(r"serotype_.*\(%\).*")),
)

calculated_seroprev_data = calculate_state_seroprevalence(calculated_count_no_seroprev_data)


function compare_calculations(calculated_count_data, calculated_seroprev_data)
    strp_count_data =
        rename(
        s -> replace(s, r"(.*)_calculated$" => s"\1"),
        calculated_count_data
    )

    strp_seroprev_data = select(
        rename(
            s -> replace(s, r"(.*)_calculated$" => s"\1"),
            calculated_seroprev_data
        ), names(strp_count_data)
    )

    @show strp_count_data == strp_seroprev_data && return

    @assert names(strp_count_data) == names(strp_seroprev_data)

    for j in 2:ncol(strp_count_data)
        println(hcat(strp_count_data[!, [1, j]], strp_seroprev_data[!, j]))
    end
    return
end


compare_calculations(calculated_count_data, calculated_seroprev_data)


#%%
# runic: off
# clean column names
# correct state names
# check no duplicated states
# assert counts are ints and seroprevs are floats (use dispatch later)
# if total pre/post counts & serotype counts, calculate serotype seroprevs
	# if serotype seroprevs exist, assert approx equal calculated serotype seroprevs
# if total pre/post counts & serotype seroprovs, calculate state counts
# if no total pre/post counts
	# if serotype counts (calculated or provided), calculate total pre/post counts
	# else warn
# check if has totals row
	# if yes, check if totals are correct (requires calculated serotype counts for weighted seroprev totals)
		# if yes, remove totals row
			# else, error
# runic: on

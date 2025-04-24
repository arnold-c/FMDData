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
calculate_state_counts(aggregated_nadcp_2)

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

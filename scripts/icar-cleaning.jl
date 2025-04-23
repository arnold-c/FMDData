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
check_duplicated_states(nadcp_2)

correct_all_state_names(
    filter(:states_ut => s -> s != "Total", nadcp_2)
)

# runic: off
# clean column names
# check no duplicated states
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

using DataFrames
using Try
using OrderedCollections: OrderedDict
using DrWatson: findproject

@testset verbose = true "icar-cleaning-functions.jl" begin
    test_dir(args...) = joinpath(findproject(), args...)

    dir = test_dir()
    filename = "test-data.csv"
    data = Try.@? load_csv(
        filename,
        dir,
        DataFrame
    )
    cleaned_colname_data = Try.@? clean_colnames(data)

    renamed_aggregated_counts_df = Try.@? rename_aggregated_pre_post_counts(cleaned_colname_data)

    cleaned_states_data = Try.@? correct_all_state_names(
        renamed_aggregated_counts_df,
        :states_ut,
        FMDData.states_dict
    )

end

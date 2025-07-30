using FMDData
using DataFrames
using Try
using DrWatson: datadir

begin
    diva_data = Try.@? load_csv(
        "diva-data.csv",
        FMDData.input_dir(),
        DataFrame
    )

    rename!(diva_data, :state => :states_ut)
    rename!(diva_data, :year => :sample_year)
    rename!(diva_data, :n_tested => :diva_count_all)
    rename!(diva_data, :n_positive => :diva_count_positive)
    rename!(diva_data, :diva_percent => :diva_pct)
    select!(diva_data, Not("Column1"))
    transform!(diva_data, :diva_pct => p -> round.(p; digits = 2); renamecols = false)
    diva_data = Try.@? FMDData.correct_all_state_names(diva_data)

    FMDData.add_state_code!(diva_data)
    Try.@? sort_columns!(diva_data)
    Try.@? sort_states!(diva_data)

    Try.@? write_csv("clean_diva-data.csv", datadir("diva"), diva_data)
end

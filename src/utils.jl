using DrWatson: DrWatson
export input_dir,
    icar_inputs_dir

input_dir(args...) = DrWatson.projectdir("inputs", args...)
icar_inputs_dir(args...) = input_dir("ICAR-Reports", "extracted-seroprevalence-tables", args...)

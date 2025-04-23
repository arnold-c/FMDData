"""
Placeholder for a short summary about FMDData.
"""
module FMDData

include("./utils.jl")
include("./icar-cleaning-functions.jl")

@static if false
    include("../scripts/icar-cleaning.jl")
end

end # module

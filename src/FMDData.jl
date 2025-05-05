"""
Placeholder for a short summary about FMDData.
"""
module FMDData

include("./utils.jl")
include("./state-keys.jl")
include("./icar-cleaning-functions.jl")

# Include to help LSP work in files outside of the src/ dir
@static if false
    include("../scripts/icar-cleaning.jl")
    include("../test/icar-cleaning-functions.jl")
end

end # module

# see documentation at https://juliadocs.github.io/Documenter.jl/stable/

using Documenter
using DocumenterVitepress
using FMDData

makedocs(
    modules = [FMDData],
    repo = Remotes.GitHub("arnold-c", "FMDData.jl"),
    format = MarkdownVitepress(;
        repo = "github.com/arnold-c/FMDData.jl.git",
    ),
    authors = "arnold-c",
    sitename = "FMDData.jl",
    pages = Any[
        "index.md",
        "exported.md",
        "internal.md",
    ],
    clean = true,
    checkdocs = :exports,
    remotes = nothing
)

# Some setup is needed for documentation deployment, see “Hosting Documentation” and
# deploydocs() in the Documenter manual for more information.
deploydocs(
    repo = "github.com/arnold-c/FMDData.jl.git",
    target = "build", # this is where Vitepress stores its output
    devbranch = "main",
    branch = "gh-pages",
    push_preview = true
)

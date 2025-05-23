# see documentation at https://juliadocs.github.io/Documenter.jl/stable/

using Documenter
using DocumenterVitepress
using FMDData

Documenter.makedocs(
    modules = [FMDData],
    repo = Remotes.GitHub("arnold-c", "FMDData"),
    format = MarkdownVitepress(;
        repo = "github.com/arnold-c/FMDData.git",
    ),
    authors = "arnold-c",
    sitename = "FMDData",
    pages = Any[
        "index.md",
        "exported.md",
        "internal.md",
    ],
)

# Some setup is needed for documentation deployment, see “Hosting Documentation” and
# deploydocs() in the Documenter manual for more information.
DocumenterVitepress.deploydocs(
    repo = "github.com/arnold-c/FMDData.git",
    target = "build", # this is where Vitepress stores its output
    devbranch = "main",
    branch = "docs",
    push_preview = true
)

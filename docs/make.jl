# see documentation at https://juliadocs.github.io/Documenter.jl/stable/

using Documenter
using DocumenterVitepress
using FMDData

gh_user = "arnold-c"
gh_repo_name = "FMDData"
repo = "github.com/$gh_user/$gh_repo_name.git"
devbranch = "main"
devurl = "dev"
docsbranch = "docs"
deploy_url = "https://fmddata.callumarnold.com"

Documenter.makedocs(
    modules = [FMDData],
    repo = Remotes.GitHub(gh_user, gh_repo_name),
    format = MarkdownVitepress(;
        repo = repo,
        devbranch = devbranch,
        devurl = devurl,
        deploy_url = deploy_url,
        md_output_path = ".",
        build_vitepress = false,
    ),
    authors = "arnold-c",
    sitename = "FMDData",
    pages = Any[
        "Home" => "index.md",
        "External Functions & Objects" => [
            "common.md",
            "icar-cleaning.md",
            "icar-processing.md",
        ],
        "internal.md",
        "doc-instructions.md",
    ],
)

# Some setup is needed for documentation deployment, see “Hosting Documentation” and
# deploydocs() in the Documenter manual for more information.
DocumenterVitepress.deploydocs(
    repo = repo,
    target = joinpath(@__DIR__, "build"), # this is where Vitepress stores its output
    devbranch = devbranch,
    devurl = devurl,
    branch = docsbranch,
    push_preview = true
)

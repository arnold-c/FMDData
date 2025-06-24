# Building the Documentation Locally

This guide provides instructions on how to build and view the documentation website for `FMDData.jl` on your local machine.

## Prerequisites

1.  **Git:** You need Git to clone the repository. You can download it from [git-scm.com](https://git-scm.com/downloads).
2.  **Julia:** Ensure you have a recent version of Julia installed. You can download it from [julialang.org](https://julialang.org/downloads/).

## Setup

1.  **Clone the repository:**

Open a terminal and clone the `FMDData` repository, then navigate into the `docs` directory:

```bash
git clone https://github.com/arnold-c/FMDData.git
cd FMDData/docs
```

2.  **Install Dependencies:**
The project's documentation has both Julia and Javascript dependencies. From the `FMDData/docs` directory:

First, install the Javascript dependencies using `npm`:
```bash
npm install
```

Next, install the Julia dependencies. Start a Julia REPL in the `docs` directory (`julia`), then press `]` to enter the package manager.
```julia-repl
(@v1.x) pkg> activate .
(docs) pkg> instantiate
```
Press backspace to return to the Julia prompt.

## Live Previewing the Documentation

The project is already configured for live previews. To view your changes as you make them, you will need two separate terminal sessions, both running inside the `FMDData/docs` directory.

**Terminal 1: Run `LiveServer`**

In the first terminal, start Julia and run the following commands. This will serve the documentation files and automatically rebuild them when you save a change.
```julia
using LiveServer
servedocs(foldername=pwd())
```

**Terminal 2: Run `DocumenterVitepress`**

In the second terminal, start another Julia session and run the following command. This will watch for the rebuilt files and update the Vitepress site that you view in your browser.
```julia
using DocumenterVitepress
DocumenterVitepress.dev_docs("build", md_output_path = "")
```

With both scripts running, you can now edit the source `.md` files in the `docs/src` directory. Your changes will automatically appear in your browser.

### Potential Issues

If your browser redirects to a page mentioning `REPLACE_ME_DOCUMENTER_VITEPRESS`, it means `DocumenterVitepress` is not detecting the file changes from `LiveServer` quickly enough. To fix this, you can add a small delay in the documentation source file you are editing.

For example:

````
```@example
sleep(0.1)
```
````

A delay of `0.1` seconds is usually sufficient, but you may need to adjust it if the problem continues.

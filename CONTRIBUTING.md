# Welcome

First of all thank you! If you are reading this not only you are interested in this project but you are also willing to put the effort in and you have our appreciation.

# Feature/Pull Requests requirements

Any change request/pull request will have to:
* be compatible with our [Roadmap](./README.md), each version has a specific set of features planned
* be included in the architecture and play nice with our future plans
* respect the coding guidelines

We use Github issues for tasks management so please file an issue before making a pull request or at least have a discussions with us.

# The Code

To keep things simple: 
* for source code we use Linux file systems (to avoid git conflicts and issues)
* We try to achieve at least 80% of code coverage with unit or integration tests (we use https://github.com/stretchr/testify ) on non-generated code 
* We use the `alpha` Git branch as `master` for now
* Pull Requests have to be reviewed by a core contributor 
* We enforce a set of linters using [golangci-lint](https://golangci-lint.run/)
* Must be compatible with `GoDoc` 
* We enforce a Git linear history (only squash rebase allowed)


# Dependencies and structure

* [Logrus](https://pkg.go.dev/github.com/sirupsen/logrus?tab=doc) for logging
* [Testify](https://pkg.go.dev/mod/github.com/stretchr/testify@v1.5.1) for unit tests
* [Goswagger.io](https://goswagger.io/) for HTTP generators
* [Godoc](https://blog.golang.org/godoc) for documentation 
* [draw.io](https://app.diagrams.net/) for diagrams see [docs](./docs)
* [Github Actions](https://github.com/features/actions) for CI pipelines
* [docker hub](https://hub.docker.com/) for docker images
* [Makefile](./Makefile) that is the entry-point for any repetitive task like linting and building
* We use `go` minimum version 1.14
* We use `go modules` as subdirectories

## Local Setup (recommended)

* linux compatible (Linux, MacOS or Windows WSL)
* `Makefile`
* `golangci-lint` https://github.com/golangci/golangci-lint minium version v1.26.0   (for Goland there is a plugin https://plugins.jetbrains.com/plugin/12496-go-linter )
```
   GO111MODULE=on go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.26.0
   golangci-lint -v
```
* VSCode has a draw.io plugin easy to see/edit the docs folder https://marketplace.visualstudio.com/items?itemName=hediet.vscode-drawio


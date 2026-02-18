# initialize project environment
.PHONY: setup
setup:
	@sh ./scripts/setup_env.sh
	@sh ./scripts/setup_hook.sh

# format code
.PHONY: fmt
fmt:
	@goimports -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*" -not -path "./.vscode/*")
	@gofumpt -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*" -not -path "./.vscode/*")

# clean up project dependencies
.PHONY: tidy
tidy:
	@go mod tidy -v

# check code format and dependencies
.PHONY: check
check:
	@$(MAKE) --no-print-directory fmt
	@$(MAKE) --no-print-directory tidy

# lint code
.PHONY: lint
lint:
	@golangci-lint run -c ./scripts/lint/.golangci.yaml ./...

# Makefile for langsmith-otel-proxy

.PHONY: help build test test-verbose clean lint fmt vet deps check-deps

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build
build: ## Build the collector binary
	@echo "Building collector..."
	go build -o bin/collector ./cmd/collector

##@ Testing
test: ## Run all tests
	@echo "Running tests..."
	go test ./...

test-verbose: ## Run all tests with verbose output
	@echo "Running tests with verbose output..."
	go test -v ./...

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-race: ## Run tests with race detection
	@echo "Running tests with race detection..."
	go test -race ./...

##@ Code Quality
lint: ## Run golangci-lint
	@echo "Running linter..."
	golangci-lint run

fmt: ## Format Go code
	@echo "Formatting code..."
	go fmt ./...

vet: ## Run go vet
	@echo "Running go vet..."
	go vet ./...

##@ Dependencies
deps: ## Download dependencies
	@echo "Downloading dependencies..."
	go mod download

tidy: ## Tidy up dependencies
	@echo "Tidying dependencies..."
	go mod tidy

check-deps: ## Verify dependencies
	@echo "Verifying dependencies..."
	go mod verify

##@ Cleanup
clean: ## Clean build artifacts
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f coverage.out coverage.html

##@ Development
dev-setup: deps ## Set up development environment
	@echo "Setting up development environment..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi

run: build ## Build and run the collector
	@echo "Running collector..."
	./bin/collector

##@ CI/CD
ci-test: deps test vet ## Run CI tests (deps, test, vet)
	@echo "CI tests completed successfully"

ci-test-race: deps test-race vet ## Run CI tests with race detection (for local development)
	@echo "CI tests with race detection completed successfully"

ci-lint: deps lint ## Run CI linting
	@echo "CI linting completed successfully"

# Check if required tools are installed
check-tools: ## Check if required development tools are installed
	@echo "Checking required tools..."
	@command -v go >/dev/null 2>&1 || { echo "Go is not installed"; exit 1; }
	@echo "✓ Go is installed"
	@command -v golangci-lint >/dev/null 2>&1 || echo "⚠ golangci-lint is not installed (run 'make dev-setup')"
	@echo "Tool check completed"

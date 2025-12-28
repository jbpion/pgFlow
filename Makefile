# pgFlow Makefile
# Build, deploy, and manage pgFlow artifacts

# Configuration
VERSION ?= dev
OUTPUT_DIR ?= dist
PGFLOW_CONNSTR ?= $(shell echo $$PGFLOW_CONNSTR)

# Detect OS
ifeq ($(OS),Windows_NT)
	SHELL := powershell.exe
	.SHELLFLAGS := -NoProfile -Command
	RM := Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
	MKDIR := New-Item -ItemType Directory -Force -Path
	BUILD_SCRIPT := .\build-artifact.ps1
	BUILD_RUNTIME_SCRIPT := .\build-runtime.ps1
	DEPLOY_SCRIPT := .\deploy.ps1
	EXPORT_SCRIPT := .\export-pipeline.ps1
else
	SHELL := /bin/bash
	RM := rm -rf
	MKDIR := mkdir -p
	BUILD_SCRIPT := ./build-artifact.sh
	BUILD_RUNTIME_SCRIPT := ./build-runtime.sh
	DEPLOY_SCRIPT := ./deploy.sh
	EXPORT_SCRIPT := ./export-pipeline.sh
endif

.PHONY: help build runtime clean deploy deploy-runtime export-pipeline test all

# Default target
help:
	@echo "pgFlow Build System"
	@echo ""
	@echo "Targets:"
	@echo "  build              Build full pgFlow artifact (includes DSL functions)"
	@echo "  runtime            Build runtime-only artifact (execution only)"
	@echo "  deploy             Deploy full artifact to database"
	@echo "  deploy-runtime     Deploy runtime artifact to database"
	@echo "  export-pipeline    Export a pipeline as deployable SQL"
	@echo "  clean              Remove build artifacts"
	@echo "  test               Run integration tests (future)"
	@echo "  all                Build both full and runtime artifacts"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION            Version string (default: dev)"
	@echo "  OUTPUT_DIR         Output directory (default: dist)"
	@echo "  PGFLOW_CONNSTR     PostgreSQL connection string"
	@echo "  PIPELINE_NAME      Pipeline name for export"
	@echo ""
	@echo "Examples:"
	@echo "  make build VERSION=1.0.0"
	@echo "  make deploy PGFLOW_CONNSTR='postgresql://localhost/mydb'"
	@echo "  make runtime VERSION=1.0.0"
	@echo "  make export-pipeline PIPELINE_NAME=daily_orders VERSION=1.0.0"

# Build full artifact
build:
	@echo "Building pgFlow full artifact (version: $(VERSION))..."
ifeq ($(OS),Windows_NT)
	$(BUILD_SCRIPT) -Version $(VERSION) -OutputDir $(OUTPUT_DIR)
else
	$(BUILD_SCRIPT) $(VERSION) $(OUTPUT_DIR)
endif
	@echo "Build complete: $(OUTPUT_DIR)/pgflow-$(VERSION).sql"

# Build runtime-only artifact
runtime:
	@echo "Building pgFlow runtime artifact (version: $(VERSION))..."
ifeq ($(OS),Windows_NT)
	$(BUILD_RUNTIME_SCRIPT) -Version $(VERSION) -OutputDir $(OUTPUT_DIR)
else
	$(BUILD_RUNTIME_SCRIPT) $(VERSION) $(OUTPUT_DIR)
endif
	@echo "Runtime build complete: $(OUTPUT_DIR)/pgflow-$(VERSION)-runtime.sql"

# Build both artifacts
all: build runtime
	@echo "All artifacts built successfully"

# Deploy full artifact
deploy: build
	@echo "Deploying pgFlow full artifact..."
ifndef PGFLOW_CONNSTR
	$(error PGFLOW_CONNSTR is not set. Set environment variable or use: make deploy PGFLOW_CONNSTR='your_connection_string')
endif
ifeq ($(OS),Windows_NT)
	$(DEPLOY_SCRIPT) -Version $(VERSION) -ConnectionString "$(PGFLOW_CONNSTR)"
else
	$(DEPLOY_SCRIPT) -v $(VERSION) -c "$(PGFLOW_CONNSTR)"
endif

# Deploy runtime artifact
deploy-runtime: runtime
	@echo "Deploying pgFlow runtime artifact..."
ifndef PGFLOW_CONNSTR
	$(error PGFLOW_CONNSTR is not set. Set environment variable or use: make deploy-runtime PGFLOW_CONNSTR='your_connection_string')
endif
ifeq ($(OS),Windows_NT)
	$(DEPLOY_SCRIPT) -Version $(VERSION) -ConnectionString "$(PGFLOW_CONNSTR)" -Runtime
else
	$(DEPLOY_SCRIPT) -v $(VERSION) -c "$(PGFLOW_CONNSTR)" --runtime
endif

# Export pipeline as deployable SQL
export-pipeline:
	@echo "Exporting pipeline..."
ifndef PIPELINE_NAME
	$(error PIPELINE_NAME is required. Use: make export-pipeline PIPELINE_NAME=your_pipeline)
endif
ifndef PGFLOW_CONNSTR
	$(error PGFLOW_CONNSTR is not set. Set environment variable or use: make export-pipeline PGFLOW_CONNSTR='your_connection_string' PIPELINE_NAME='your_pipeline')
endif
ifeq ($(OS),Windows_NT)
	$(EXPORT_SCRIPT) -PipelineName $(PIPELINE_NAME) -Version $(VERSION) -ConnectionString "$(PGFLOW_CONNSTR)"
else
	$(EXPORT_SCRIPT) -n $(PIPELINE_NAME) -v $(VERSION) -c "$(PGFLOW_CONNSTR)"
endif

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
ifeq ($(OS),Windows_NT)
	$(RM) $(OUTPUT_DIR)
else
	$(RM) $(OUTPUT_DIR)
endif
	@echo "Clean complete"

# Test target (placeholder for future implementation)
test:
	@echo "Running tests..."
	@echo "Tests not yet implemented"

# Development workflow: build and deploy to local database
dev: build
	@echo "Deploying to development environment..."
ifndef PGFLOW_CONNSTR
	$(error PGFLOW_CONNSTR is not set for development deployment)
endif
ifeq ($(OS),Windows_NT)
	$(DEPLOY_SCRIPT) -Version dev -ConnectionString "$(PGFLOW_CONNSTR)"
else
	$(DEPLOY_SCRIPT) -v dev -c "$(PGFLOW_CONNSTR)"
endif
	@echo "Development deployment complete"

# Production workflow: build runtime and show deployment instructions
prod: runtime
	@echo ""
	@echo "Runtime artifact built: $(OUTPUT_DIR)/pgflow-$(VERSION)-runtime.sql"
	@echo ""
	@echo "To deploy to production:"
	@echo "  1. Deploy runtime artifact: make deploy-runtime PGFLOW_CONNSTR='<prod_connection>'"
	@echo "  2. Export pipelines: make export-pipeline PIPELINE_NAME=<name> VERSION=$(VERSION)"
	@echo "  3. Deploy exported pipelines via migration system"

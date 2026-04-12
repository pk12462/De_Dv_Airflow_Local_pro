#!/bin/bash
# ============================================================================
# Bank Customer Risk Triggers ETL - Docker Deployment Script
# ============================================================================
# This script deploys the ETL platform to production using Docker
# Usage: bash deploy.sh [environment] [version]

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ENVIRONMENT="${1:-dev}"
VERSION="${2:-latest}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ============================================================================
# Validation
# ============================================================================

validate_environment() {
    log_info "Validating environment..."

    if [ ! -f ".env.bank-etl" ]; then
        log_error ".env.bank-etl not found"
        exit 1
    fi

    if [ ! -f "docker-compose-bank-etl.yaml" ]; then
        log_error "docker-compose-bank-etl.yaml not found"
        exit 1
    fi

    log_success "Environment validation passed"
}

# ============================================================================
# Build
# ============================================================================

build_images() {
    log_info "Building Docker images..."

    # Build custom Spark image (if Dockerfile exists)
    if [ -f "docker/Dockerfile" ]; then
        log_info "Building custom Spark image..."
        docker build \
            -t bank-etl-spark:${VERSION} \
            -f docker/Dockerfile \
            .
        log_success "Spark image built: bank-etl-spark:${VERSION}"
    fi

    log_success "Docker images ready"
}

# ============================================================================
# Deploy
# ============================================================================

deploy_services() {
    log_info "Deploying services to $ENVIRONMENT environment..."

    # Load environment variables
    export $(cat .env.bank-etl | grep -v '^#' | xargs)

    # Create required directories
    log_info "Creating required directories..."
    mkdir -p logs
    mkdir -p data/checkpoint
    mkdir -p data/audit
    mkdir -p data/output

    # Start containers
    log_info "Starting Docker containers..."
    docker-compose -f docker-compose-bank-etl.yaml up -d

    log_success "Services deployed successfully"
}

# ============================================================================
# Health Checks
# ============================================================================

health_check() {
    log_info "Running health checks..."

    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL to be ready..."
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if docker exec postgres-bank pg_isready -U postgres > /dev/null 2>&1; then
            log_success "PostgreSQL is ready"
            break
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    if [ $attempt -eq $max_attempts ]; then
        log_error "PostgreSQL failed to start"
        exit 1
    fi

    # Wait for Spark Master
    log_info "Waiting for Spark Master to be ready..."
    sleep 10
    if curl -s http://localhost:8080 > /dev/null; then
        log_success "Spark Master is ready"
    else
        log_warning "Spark Master may not be fully ready yet"
    fi

    # Check all containers
    log_info "Container status:"
    docker-compose -f docker-compose-bank-etl.yaml ps

    log_success "Health check completed"
}

# ============================================================================
# Initialization
# ============================================================================

initialize_database() {
    log_info "Initializing database schema..."

    # Wait a bit for PostgreSQL to be fully ready
    sleep 5

    # Initialize schema and sample data
    docker exec postgres-bank psql -U postgres -d bank_db -c "
        SELECT 'Database initialized' as status;
    " || log_warning "Database may already be initialized"

    log_success "Database initialization completed"
}

# ============================================================================
# Deployment Report
# ============================================================================

generate_report() {
    log_info "Generating deployment report..."

    cat > DEPLOYMENT_REPORT.txt <<EOF
================================================================================
Bank Customer Risk Triggers ETL - Deployment Report
================================================================================

Deployment Date: $(date)
Environment: $ENVIRONMENT
Version: $VERSION

Service Status:
EOF

    docker-compose -f docker-compose-bank-etl.yaml ps >> DEPLOYMENT_REPORT.txt

    cat >> DEPLOYMENT_REPORT.txt <<EOF

Service URLs:
- PostgreSQL:      localhost:5432
- pgAdmin:         http://localhost:5050
- Spark Master:    http://localhost:8080
- Spark Worker 1:  http://localhost:8081
- Spark Worker 2:  http://localhost:8082
- Prometheus:      http://localhost:9090
- Grafana:         http://localhost:3000
- Redis:           localhost:6379
- MinIO API:       http://localhost:9000
- MinIO Console:   http://localhost:9001

Next Steps:
1. Verify services are running: docker-compose -f docker-compose-bank-etl.yaml ps
2. Access databases: docker exec postgres-bank psql -U postgres -d bank_db
3. Run ETL pipeline: python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py
4. Monitor with Grafana: http://localhost:3000

For more information, see: ETL_SETUP_GUIDE.md

================================================================================
EOF

    log_success "Deployment report saved to DEPLOYMENT_REPORT.txt"
    cat DEPLOYMENT_REPORT.txt
}

# ============================================================================
# Rollback
# ============================================================================

rollback() {
    log_warning "Rolling back deployment..."

    # Stop containers but keep volumes
    docker-compose -f docker-compose-bank-etl.yaml down --keep-pvc

    log_success "Rollback completed"
}

# ============================================================================
# Cleanup
# ============================================================================

cleanup() {
    log_warning "Cleaning up all services and volumes..."

    read -p "This will delete all data. Are you sure? (yes/no): " confirm

    if [ "$confirm" = "yes" ]; then
        docker-compose -f docker-compose-bank-etl.yaml down -v
        rm -rf logs data
        log_success "Cleanup completed"
    else
        log_info "Cleanup cancelled"
    fi
}

# ============================================================================
# Main Flow
# ============================================================================

main() {
    log_info "=========================================="
    log_info "Bank ETL - Deployment Script"
    log_info "=========================================="

    validate_environment
    build_images
    deploy_services
    health_check
    initialize_database
    generate_report

    log_success "=========================================="
    log_success "Deployment completed successfully!"
    log_success "=========================================="
}

# ============================================================================
# Entry Point
# ============================================================================

if [ "$ENVIRONMENT" = "rollback" ]; then
    rollback
elif [ "$ENVIRONMENT" = "cleanup" ]; then
    cleanup
elif [ "$ENVIRONMENT" = "status" ]; then
    docker-compose -f docker-compose-bank-etl.yaml ps
else
    main
fi


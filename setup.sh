#!/bin/bash
# ============================================================================
# Bank Customer Risk Triggers ETL - Setup Script
# ============================================================================
# This script sets up the complete ETL environment with Docker and dependencies
# Usage: bash setup.sh

set -e

echo "════════════════════════════════════════════════════════════════════════════"
echo "Bank Customer Risk Triggers ETL - Setup Script"
echo "════════════════════════════════════════════════════════════════════════════"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is not installed. Please install Python 3 first.${NC}"
    exit 1
fi

echo -e "${GREEN}All prerequisites are installed${NC}"

# Load environment variables
echo -e "${YELLOW}Loading environment variables...${NC}"
if [ -f ".env.bank-etl" ]; then
    export $(cat .env.bank-etl | grep -v '^#' | xargs)
    echo -e "${GREEN}Environment variables loaded from .env.bank-etl${NC}"
else
    echo -e "${RED}.env.bank-etl not found. Creating a default one...${NC}"
    cp .env.bank-etl .env.bank-etl.example || echo "No default .env.bank-etl to copy"
fi

# Install Python dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r requirements.txt

echo -e "${GREEN}Python dependencies installed${NC}"

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p logs
mkdir -p data/checkpoint
mkdir -p data/audit
mkdir -p data/output

echo -e "${GREEN}Directories created${NC}"

# Start Docker containers
echo -e "${YELLOW}Starting Docker containers...${NC}"
docker-compose -f docker-compose-bank-etl.yaml up -d

echo -e "${GREEN}Docker containers started${NC}"

# Wait for PostgreSQL to be ready
echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec postgres-bank pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}PostgreSQL is ready${NC}"
        break
    fi
    attempt=$((attempt + 1))
    echo "Waiting for PostgreSQL... ($attempt/$max_attempts)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo -e "${RED}PostgreSQL failed to start after $max_attempts attempts${NC}"
    exit 1
fi

# Initialize PostgreSQL schema
echo -e "${YELLOW}Initializing PostgreSQL schema...${NC}"
docker exec postgres-bank psql -U postgres -d bank_db -f /docker-entrypoint-initdb.d/01-schema.sql || true

echo -e "${GREEN}PostgreSQL schema initialized${NC}"

# Build Scala project (optional)
echo -e "${YELLOW}Building Scala project...${NC}"
cd batch_apps/spark_batch
if [ -f "build.sbt" ]; then
    sbt package || echo "Scala build skipped (optional)"
    cd ../../
    echo -e "${GREEN}Scala project built${NC}"
else
    echo -e "${YELLOW}build.sbt not found, skipping Scala build${NC}"
fi

# Display setup summary
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo -e "${GREEN}✓ Setup completed successfully!${NC}"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Service URLs:"
echo "  - PostgreSQL:      localhost:5432 (user: postgres, password: postgres)"
echo "  - pgAdmin:         http://localhost:5050 (admin@example.com / admin)"
echo "  - Spark Master:    http://localhost:8080"
echo "  - Spark Worker 1:  http://localhost:8081"
echo "  - Spark Worker 2:  http://localhost:8082"
echo "  - Prometheus:      http://localhost:9090"
echo "  - Grafana:         http://localhost:3000 (admin / admin)"
echo "  - Redis:           localhost:6379"
echo "  - MinIO API:       http://localhost:9000"
echo "  - MinIO Console:   http://localhost:9001"
echo ""
echo "Next Steps:"
echo "  1. Run the ETL pipeline:"
echo "     python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py"
echo ""
echo "  2. Or run with custom config:"
echo "     python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \\"
echo "       --config pipelines/configs/bank_cust_risk_triggers_config.yaml \\"
echo "       --date 2026-04-12"
echo ""
echo "  3. Check data in PostgreSQL:"
echo "     docker exec postgres-bank psql -U postgres -d bank_db"
echo "     SELECT COUNT(*) FROM cust_risk_triggers;"
echo ""
echo "  4. Stop all services:"
echo "     docker-compose -f docker-compose-bank-etl.yaml down"
echo ""


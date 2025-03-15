#!/bin/bash
set -e

# Apply custom PostgreSQL configurations from environment variables if provided
if [ -n "$PG_SHARED_BUFFERS" ] || [ -n "$PG_WORK_MEM" ] || [ -n "$PG_MAINTENANCE_WORK_MEM" ]; then
  # Wait for PostgreSQL to start
  function pg_setup() {
    # Create a custom postgresql.conf file
    cat > /tmp/custom_postgresql.conf << EOF
# Custom PostgreSQL configuration
shared_buffers = ${PG_SHARED_BUFFERS:-128MB}
work_mem = ${PG_WORK_MEM:-4MB}
maintenance_work_mem = ${PG_MAINTENANCE_WORK_MEM:-64MB}
EOF
    
    # Apply the custom configuration
    echo "include = '/tmp/custom_postgresql.conf'" >> "$PGDATA/postgresql.conf"
    
    echo "PostgreSQL custom configuration applied."
  }
  
  # Check if PostgreSQL is initialized
  if [ -s "$PGDATA/PG_VERSION" ]; then
    pg_setup
  else
    # If not initialized, set up a post-initialization hook
    echo "PostgreSQL data directory is not yet initialized. Will apply configuration after initialization."
    cat > /docker-entrypoint-initdb.d/zzz_apply_config.sh << 'EOF'
#!/bin/bash
pg_setup
EOF
    chmod +x /docker-entrypoint-initdb.d/zzz_apply_config.sh
  fi
fi

# Call the original entrypoint
exec docker-entrypoint.sh "$@"
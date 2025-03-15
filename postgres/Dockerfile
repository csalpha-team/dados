# Use the official PostgreSQL image
FROM postgres:15

# These environment variables can be overridden by docker-compose
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=testdb

# Create a directory for initialization scripts
RUN mkdir -p /docker-entrypoint-initdb.d
# Copy the initialization SQL script into the container
COPY ./init/init.sql /docker-entrypoint-initdb.d/


# Create a startup script to apply custom configurations at runtime
COPY ./custom-entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

# Expose the PostgreSQL port
EXPOSE 5432

# Use custom entrypoint to apply runtime configurations
ENTRYPOINT ["/custom-entrypoint.sh"]
CMD ["postgres"]
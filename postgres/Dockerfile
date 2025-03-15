FROM postgres:15

# Cria diretório para armazenar scripts de setup dentro do container
RUN mkdir -p /docker-entrypoint-initdb.d

# Copia arquivo de definição dos bancos de dados para o container
COPY ./init/init.sql /docker-entrypoint-initdb.d/init.sql.template
COPY ./init/create-databases.sh /docker-entrypoint-initdb.d/

# Copia scripts para implementar configurações do banco parametrizadas em .env e 
COPY ./custom-entrypoint.sh /custom-entrypoint.sh

# Define permissão de execução para o script
RUN chmod +x /custom-entrypoint.sh

EXPOSE 5432

ENTRYPOINT ["/custom-entrypoint.sh"]
CMD ["postgres"]
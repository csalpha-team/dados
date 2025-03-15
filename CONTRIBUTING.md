# Guia de Contribuição

Este documento fornece instruções detalhadas sobre como configurar o ambiente de desenvolvimento local para trabalhar com nossa arquitetura de zonas de dados baseada em PostgreSQL.

## Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- [Docker](https://www.docker.com/get-started) (versão 19.03 ou superior)
- [Docker Compose](https://docs.docker.com/compose/install/) (versão 1.25 ou superior)
- Git

## Configuração do Ambiente Local

### 1. Clone o Repositório

```bash
git clone https://github.com/sua-organizacao/seu-repositorio.git
cd seu-repositorio
```

### 2. Configure as Variáveis de Ambiente

Copie o arquivo .env.examplo e crie o `.env` na raiz do projeto:

### 3. Inicialize os Contêineres Docker

Execute o comando a seguir para iniciar os contêineres PostgreSQL e PgAdmin:

```bash
docker-compose up -d
```

Isso iniciará:
- Um contêiner PostgreSQL com quatro bancos de dados (um para cada zona)
- Um contêiner PgAdmin para gerenciamento visual dos bancos de dados

### 4. Verifique a Inicialização

Verifique se os contêineres estão rodando corretamente:

```bash
docker-compose ps

# Saída esperada:
#           Name                      Command              State           Ports
# ------------------------------------------------------------------------------------------
# postgres_csalpha         /custom-entrypoint.sh postgres   Up      0.0.0.0:5432->5432/tcp
# pgadmin_container        /entrypoint.sh                   Up      0.0.0.0:5050->80/tcp
```

Você deverá ver dois contêineres em estado "Up":
- `postgres_csalpha`
- Um contêiner PgAdmin

### 5. Acesse o PgAdmin

Abra seu navegador e acesse o PgAdmin:

```
http://localhost:5050
```

Faça login usando as credenciais definidas no arquivo `.env`:
- **Email**: csalpha@gmail.com (ou o que você definiu em `PGADMIN_DEFAULT_EMAIL`)
- **Senha**: test1234 (ou o que você definiu em `PGADMIN_DEFAULT_PASSWORD`)

### 6. Configure a Conexão com o PostgreSQL no PgAdmin

1. Após fazer login, clique com o botão direito em "Servers" no painel da esquerda e selecione "Create" > "Server..."

2. Na aba "General", dê um nome ao servidor (ex: "PostgreSQL CSAlpha")

3. Na aba "Connection", configure:
   ```
   Host name/address: postgres
   Port: 5432
   Maintenance database: postgres
   Username: postgres (ou o que você definiu em POSTGRES_USER)
   Password: postgres (ou o que você definiu em POSTGRES_PASSWORD)
   ```

4. Clique em "Save"

### 7. Explore os Bancos de Dados

Agora você deve ver quatro bancos de dados, um para cada zona:

```
● postgresql_csalpha
  ├── Databases
  │   ├── postgres (sistema)
  │   ├── csalpha_raw
  │   │   └── Schemas
  │   │       └── raw_data
  │   ├── csalpha_trusted
  │   │   └── Schemas
  │   │       └── trusted_data
  │   ├── csalpha_agregated
  │   │   └── Schemas
  │   │       └── agregated_data
  │   └── csalpha_output
  │       └── Schemas
  │           └── output_data
  └── ...
```

Cada banco de dados contém um schema específico que você pode usar para armazenar dados relacionados à respectiva zona.

## Entendendo os Scripts de Inicialização

### Fluxo de Inicialização

```
+---------------------+     +----------------------+     +------------------+
| Iniciar PostgreSQL  | --> | Processar template   | --> | Criar bancos     |
| Esperar prontidão   |     | Substituir variáveis |     | Criar schemas    |
+---------------------+     +----------------------+     +------------------+
```

### Script Principal (`create-databases.sh`)

Este script é executado durante a inicialização do contêiner PostgreSQL e cria os quatro bancos de dados para as zonas:

O script:
1. Espera pelo PostgreSQL estar pronto para aceitar conexões
2. Processa o template SQL substituindo variáveis de ambiente
3. Executa o SQL para criar os bancos de dados e schemas

### Template SQL (`init.sql.template`)

Este arquivo contém os comandos SQL para criar os bancos de dados e schemas, usando variáveis de ambiente que são substituídas durante a execução:

## Comandos Úteis

### Gerenciamento de Contêineres

```bash
# Iniciar os contêineres
docker compose up -d

# Parar os contêineres
docker compose stop

# Reiniciar os contêineres
docker-compose restart

# Parar e remover os contêineres
docker compose down

# Parar e remover os contêineres e volumes
docker compose down -v
# Ver logs dos contêineres
docker compose logs

# Ver logs específicos do PostgreSQL
docker compose logs postgres
```

### Interagindo com o PostgreSQL diretamente

```bash
# Conectar ao shell do contêiner PostgreSQL
docker exec -it postgres_csalpha bash

# Conectar ao PostgreSQL via psql
docker exec -it postgres_csalpha psql -U postgres

# Listar todos os bancos de dados
docker exec -it postgres_csalpha psql -U postgres -c "\l"

# Executar comando SQL específico
docker exec -it postgres_csalpha psql -U postgres -c "SELECT version();"
```

## Solução de Problemas

### PgAdmin não conecta ao PostgreSQL

Se você encontrar problemas ao conectar o PgAdmin ao PostgreSQL:

1. Verifique se ambos os contêineres estão rodando:
   ```bash
   docker-compose ps
   ```

2. Certifique-se de usar "postgres" como nome do host na configuração de conexão do PgAdmin, não "localhost"

3. Verifique as credenciais no arquivo `.env` e na configuração da conexão

4. Verifique os logs do PostgreSQL:
   ```bash
   docker-compose logs postgres
   ```

### Bancos de dados não foram criados

Se os bancos de dados das zonas não aparecerem:

1. Verifique os logs de inicialização:
   ```bash
   docker-compose logs postgres | grep "Creating zone architecture databases"
   ```

2. Certifique-se de que as variáveis de ambiente estão definidas corretamente no arquivo `.env`

3. Tente recriar os contêineres:
   ```bash
   docker-compose down
   docker-compose up -d --build
   ```

## Fluxo de Contribuição

1. Crie um branch para sua feature ou correção:
   ```bash
   git checkout -b feature/sua-feature
   ```

2. Faça suas alterações no código

3. Execute testes locais usando o ambiente Docker

4. Envie um Pull Request para revisão


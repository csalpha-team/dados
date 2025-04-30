# Sobre o repositório
Este repositório implementa uma arquitetura de zonas para processamento de dados usando PostgreSQL. A arquitetura é projetada para permitir o processamento de dados em diferentes estágios, desde dados brutos até dados prontos para uso no algoritmo e o armazenamento de saídas.


## Arquitetura de Zonas para Processamento de Dados

### Visão Geral

Este repositório implementa uma arquitetura de zonas para processamento de dados usando PostgreSQL. A arquitetura é projetada para permitir o processamento de dados em diferentes estágios, desde dados brutos até dados prontos para uso em algoritmos e visualizações.

### Zonas de Dados

Nossa arquitetura é composta por quatro zonas principais, cada uma representada por um banco de dados PostgreSQL dedicado:

#### 1. Zona de Dados Brutos (`DB_RAW_ZONE`)

```
+-----------------+
| ZONA RAW        |
+-----------------+
| Dados originais |
| Sem tratamento  |
| Formato bruto   |
+-----------------+
```

- **Propósito**: Armazenar dados exatamente como foram recebidos das fontes originais
- **Características**: 
  - Sem transformações ou limpezas
  - Preservação do formato original
  - Rastreabilidade completa
- **Schema**: `DB_RAW_ZONE`

#### 2. Zona de Dados Tratados (`DB_SILVER_ZONE`)

```
+---------------------+
| ZONA SILVER        |
+---------------------+
| Dados validados     |
| Limpeza básica      |
| Formato padronizado |
+---------------------+
```

- **Propósito**: Armazenar dados após validação e limpeza básica
- **Características**:
  - Correção de erros evidentes
  - Padronização de formatos
  - Remoção de duplicidades
- **Schema**: `DB_SILVER_ZONE`

#### 3. Zona de Dados Agregados (`DB_AGREGATED_ZONE`)

```
+---------------------+
| ZONA GOLD       |
+---------------------+
| Dados processados   |
| Cálculos derivados  |
| Dados agregados     |
+---------------------+
```

- **Propósito**: Armazenar dados processados, transformados e agregados que serão utilizados no algoritmo
- **Características**:
  - Cálculos derivados
  - Agregações e sumarizações
  - Enriquecimento de dados
- **Schema**: `DB_AGREGATED_ZONE`

#### 4. Zona de Saída do Algoritmo (`DB_MATRICES_ZONE`)

```
+---------------------+
| ZONA MATRICES        |
+---------------------+
| Armazena matrizes   |
| Dados para consumo  |
| Saídas otimizadas   |
+---------------------+
```

- **Propósito**: Armazenar resultados finais prontos para consumo por aplicações e elaboração de relatórios

- **Características**:
  - Matrizes de insumo produto
  - Resultados do [algoritmo](https://github.com/csalpha-team/csalpha) 
- **Schema**: `output_data`

### Fluxo de Dados

```
+----------+     +-----------+     +-------------+     +-----------+
|  DADOS   |     |   ZONA    |     |    ZONA     |     |   ZONA    |
|  BRUTOS  | --> |   RAW     | --> |   SILVER   | --> | GOLD  |
+----------+     +-----------+     +-------------+     +-----------+
                                                            |
                                                            v
                                                       +-----------+
                                                       |   ZONA    |
                                                       |  OUTPUT   |
                                                       +-----------+
```

O fluxo típico de dados através das zonas segue este padrão:

1. Os dados brutos são ingeridos na **Zona Raw**
2. Processos de limpeza e validação transformam os dados para a **Zona SILVER**
3. Transformações, cálculos e agregações movem os dados para a **Zona GOLD**
4. Algoritmos específicos processam os dados e armazenam resultados na **Zona de Saída**

### Tecnologias Utilizadas

- **PostgreSQL**: Sistema de gerenciamento de banco de dados relacional
- **Docker**: Conteinerização para facilitar implantação e desenvolvimento
- **PgAdmin**: Interface gráfica para gerenciamento do PostgreSQL

### Estrutura do Repositório

```
.
├── .env                      # Variáveis de ambiente e configurações
├── docker-compose.yml        # Configuração do ambiente Docker
├── postgres/                 # Arquivos para configuração do PostgreSQL
│   ├── Dockerfile            # Imagem Docker personalizada para PostgreSQL
│   ├── custom-entrypoint.sh  # Script personalizado de inicialização
│   └── init/                 # Scripts de inicialização dos bancos
│       ├── init.sql.template # Template SQL para criação dos bancos
│       └── create-databases.sh # Script de criação dos bancos
```

### Configuração

As configurações das zonas de dados podem ser personalizadas através do arquivo `.env`. Por padrão, os bancos de dados seguem este padrão de nomenclatura:

```
DB_PREFIX=zona
DB_RAW_ZONE=${DB_PREFIX}_brutos
DB_SILVER_ZONE=${DB_PREFIX}_tratados
DB_AGREGATED_ZONE=${DB_PREFIX}_agregated
DB_OUTPUT_ZONE=${DB_PREFIX}_saida_algoritmo
```

Para instruções detalhadas sobre como executar o ambiente localmente, consulte o arquivo [CONTRIBUTING.md](CONTRIBUTING.md).


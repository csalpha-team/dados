import psycopg2
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Union, Any
from uuid import uuid4
from psycopg2.extras import execute_values


class PostgresETL:
    """
    Classe para gerenciar interação com os bancos Postgres
    """

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
        schema: str = "public",
    ):
        """
        Initialize PostgreSQL connection parameters

        Args:
            host (str): Database host address
            database (str): Database name
            user (str): Username
            password (str): Password
            port (int): Port number, defaults to 5432
            schema (str): Schema name, defaults to 'public'
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.schema = schema
        self.conn = None
        self.cursor = None
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Confgirua um logger"""
        logger = logging.getLogger("PostgresETL")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
            )
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to database {self.database} at {self.host}")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise

    def __enter__(self):
        """Context manager entry point"""
        self.connect()
        return self

    def disconnect(self):
        """Close connection to PostgreSQL database"""
        if hasattr(self, "cursor") and self.cursor:
            self.cursor.close()
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
        self.logger.info("Disconnected from database")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point"""
        self.disconnect()

    def execute_query(
        self, query: str, params: tuple = None, commit: bool = False
    ) -> None:
        """
        Execute a query on the PostgreSQL database

        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            commit (bool): Whether to commit the transaction
        """
        try:
            self.cursor.execute(query, params)
            if commit:
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            self.conn.rollback()
            raise

    def create_schema(self, schema_name: str, if_not_exists: bool = True) -> None:
        """
        Create a schema in the database

        Args:
            schema_name (str): Name of the schema to create
            if_not_exists (bool): Add IF NOT EXISTS to query
        """
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        query = f"CREATE SCHEMA {exists_clause}{schema_name}"

        self.execute_query(query, commit=True)
        self.logger.info(f"Schema {schema_name} created or already exists")

    def create_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        primary_key: Optional[Union[str, List[str]]] = None,
        if_not_exists: bool = True,
        drop_if_exists: bool = False,
    ) -> None:
        """
        Create a table in the database

        Args:
            table_name (str): Name of the table to create
            columns (Dict[str, str]): Dictionary mapping column names to their types
            primary_key (str or List[str], optional): Column(s) to use as primary key
            if_not_exists (bool): Add IF NOT EXISTS to query
            drop_if_exists (bool): Drop the table if it already exists before creating
        """
        # First, ensure the schema exists
        self.create_schema(self.schema, if_not_exists=True)

        # If drop_if_exists is True, drop the table if it exists
        if drop_if_exists:
            self.execute_query(
                f"DROP TABLE IF EXISTS {self.schema}.{table_name}", commit=True
            )
            self.logger.info(f"Dropped table {table_name} if it existed")
            # Since we're dropping the table, we don't need the IF NOT EXISTS clause
            exists_clause = ""
        else:
            exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        columns_str = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])

        query = f"CREATE TABLE {exists_clause}{self.schema}.{table_name} ({columns_str}"

        if primary_key:
            if isinstance(primary_key, list):
                pk_str = ", ".join(primary_key)
                query += f", PRIMARY KEY ({pk_str})"
            else:
                query += f", PRIMARY KEY ({primary_key})"

        query += ")"

        self.execute_query(query, commit=True)

        if drop_if_exists:
            self.logger.info(f"Table {table_name} created")
        else:
            self.logger.info(f"Table {table_name} created or already exists")

    def _chunk_records(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        start: int,
        chunk_size: int,
    ) -> List[Dict[str, Any]]:
        """Materializa apenas uma fatia dos dados como lista de dicionários.

        Para DataFrame, ``to_dict("records")`` do objeto inteiro é o que estourava
        a memória nas tabelas grandes (PAM/PEVS chegam a 12 milhões de linhas):
        a lista de dicionários custa várias vezes o tamanho do dataframe. Aqui a
        conversão acontece por fatia, então o pico fica em um chunk por vez.
        """
        if isinstance(data, pd.DataFrame):
            return data.iloc[start : start + chunk_size].to_dict("records")
        return data[start : start + chunk_size]

    def _insert_chunk(
        self,
        conn,
        insert_query: str,
        columns: List[str],
        chunk: List[Dict[str, Any]],
    ) -> int:
        """Insere uma fatia usando a conexão informada (uma por thread)."""
        values = [[record.get(col) for col in columns] for record in chunk]
        with conn.cursor() as cursor:
            execute_values(cursor, insert_query, values)
        conn.commit()
        return len(chunk)

    def load_data(
        self,
        table_name: str,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        if_exists: str = "append",
        chunk_size: int = 35399,
        max_workers: int = 1,
    ) -> int:
        """
        Load data into a PostgreSQL table

        Args:
            table_name (str): Target table name
            data (DataFrame or List[Dict]): Data to load
            if_exists (str): How to behave if the table exists
                             Options: 'fail', 'replace', 'append'
            chunk_size (int): Records per chunk when loading data
            max_workers (int): Threads de carga. 1 (padrão) usa a conexão do
                contexto, sequencialmente. Acima de 1, cada thread abre a sua
                própria conexão — psycopg2 não permite compartilhar uma conexão
                entre threads — e as fatias são inseridas em paralelo. Útil nas
                tabelas grandes (PAM/PEVS/PPM); as fatias são independentes,
                então a ordem de inserção não importa.

        Returns:
            int: Number of records inserted
        """
        total_rows = len(data) if data is not None else 0
        if not total_rows:
            self.logger.warning("No data to load")
            return 0

        if isinstance(data, pd.DataFrame):
            columns = list(data.columns)
        else:
            columns = list(data[0].keys())

        if if_exists == "replace":
            self.execute_query(
                f"TRUNCATE TABLE {self.schema}.{table_name}", commit=True
            )
            self.logger.info(f"Truncated table {table_name}")

        insert_query = f"""
        INSERT INTO {self.schema}.{table_name} ({", ".join(columns)})
        VALUES %s
        """

        self.logger.info(f"The build query is:\n{insert_query}")

        starts = list(range(0, total_rows, chunk_size))
        total_inserted = 0

        if max_workers <= 1:
            for numero, start in enumerate(starts, start=1):
                chunk = self._chunk_records(data, start, chunk_size)
                try:
                    total_inserted += self._insert_chunk(
                        self.conn, insert_query, columns, chunk
                    )
                    self.logger.info(
                        f"Inserted chunk {numero} with {len(chunk)} records"
                    )
                except Exception as e:
                    self.logger.error(f"Error inserting chunk {numero}: {e}")
                    self.conn.rollback()
                    raise
            self.logger.info(
                f"Total {total_inserted} records inserted into {table_name}"
            )
            return total_inserted

        self.logger.info(
            f"Loading {total_rows} records in {len(starts)} chunks "
            f"with {max_workers} threads"
        )

        def _worker(indices: List[int]) -> int:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
            )
            inseridos = 0
            try:
                for start in indices:
                    chunk = self._chunk_records(data, start, chunk_size)
                    try:
                        inseridos += self._insert_chunk(
                            conn, insert_query, columns, chunk
                        )
                    except Exception:
                        conn.rollback()
                        raise
            finally:
                conn.close()
            return inseridos

        # Distribui as fatias entre as threads de forma intercalada, para que
        # todas recebam aproximadamente o mesmo volume.
        particoes = [starts[i::max_workers] for i in range(max_workers)]
        particoes = [p for p in particoes if p]

        with ThreadPoolExecutor(max_workers=len(particoes)) as executor:
            futures = {executor.submit(_worker, p): p for p in particoes}
            try:
                for future in as_completed(futures):
                    total_inserted += future.result()
            except Exception as e:
                self.logger.error(f"Error inserting data into {table_name}: {e}")
                for future in futures:
                    future.cancel()
                raise

        self.logger.info(f"Total {total_inserted} records inserted into {table_name}")

        return total_inserted

    def download_data(
        self,
        query: str,
        params: tuple = None,
        fetch_size: int = 200_000,
    ) -> pd.DataFrame:
        """
        Download data from PostgreSQL to a DataFrame

        Usa cursor do lado do servidor e monta o dataframe por lotes de tuplas.
        O caminho anterior (``RealDictCursor`` + ``fetchall``) materializava um
        dicionário por linha antes de virar dataframe — em tabelas de milhões de
        linhas (PAM/PEVS) esse intermediário custava vários GB e era o que
        derrubava os flows grandes na releitura.

        Args:
            query (str): SQL query to execute
            params (tuple): Parameters for the query
            fetch_size (int): Linhas por lote trazidas do servidor

        Returns:
            DataFrame: Query results as a pandas DataFrame
        """
        cursor_name = f"download_{uuid4().hex}"
        try:
            with self.conn.cursor(name=cursor_name) as server_cursor:
                server_cursor.itersize = fetch_size
                server_cursor.execute(query, params)

                # Em cursor nomeado, ``description`` só fica disponível depois
                # do primeiro fetch — ler antes devolve None.
                colunas: List[str] = []
                partes: List[pd.DataFrame] = []
                while True:
                    lote = server_cursor.fetchmany(fetch_size)
                    if not colunas and server_cursor.description:
                        colunas = [
                            descricao[0] for descricao in server_cursor.description
                        ]
                    if not lote:
                        break
                    partes.append(pd.DataFrame(lote, columns=colunas))

            if not partes:
                df = pd.DataFrame(columns=colunas)
            elif len(partes) == 1:
                df = partes[0]
            else:
                df = pd.concat(partes, ignore_index=True, copy=False)
                partes.clear()

            self.conn.commit()
            self.logger.info(f"Downloaded {len(df)} rows from database")
            return df
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Download error: {e}")
            raise

    def download_table(
        self,
        table_name: str,
        columns: List[str] = None,
        condition: str = None,
        params: tuple = None,
        limit: int = None,
        offset: int = None,
    ) -> pd.DataFrame:
        """
        Download data from a specific table

        Args:
            table_name (str): Source table name
            columns (List[str], optional): Columns to select
            condition (str, optional): WHERE clause
            params (tuple, optional): Parameters for the condition
            limit (int, optional): LIMIT clause
            offset (int, optional): OFFSET clause

        Returns:
            DataFrame: Table data as pandas DataFrame
        """
        select_clause = "*"
        if columns:
            select_clause = ", ".join(columns)

        query = f"SELECT {select_clause} FROM {self.schema}.{table_name}"

        if condition:
            query += f" WHERE {condition}"

        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"

        return self.download_data(query, params)

import psycopg2
import pandas as pd
import logging
from typing import List, Dict, Optional, Union, Any
from psycopg2.extras import execute_values, RealDictCursor


class PostgresETL:
    """
    Classe para gerenciar interação com os bancos Postgres
    """
    def __init__(self,
        host: str,
        database: str,
        user: str,
        password: str,
        port:  int = 5432,
        schema: str = 'public'):
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
        logger = logging.getLogger('PostgresETL')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
                port=self.port
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
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        self.logger.info("Disconnected from database")
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point"""
        self.disconnect()
        
    def execute_query(self, query: str, params: tuple = None, commit: bool = False) -> None:
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
    
    def create_table(self, table_name: str, columns: Dict[str, str],
                 primary_key: Optional[Union[str, List[str]]] = None,
                 if_not_exists: bool = True,
                 drop_if_exists: bool = False) -> None:
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
            self.execute_query(f"DROP TABLE IF EXISTS {self.schema}.{table_name}", commit=True)
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
        
    def load_data(self, table_name: str, data: Union[pd.DataFrame, List[Dict[str, Any]]],
                  if_exists: str = 'append', chunk_size: int = 35399) -> int:
        """
        Load data into a PostgreSQL table
        
        Args:
            table_name (str): Target table name
            data (DataFrame or List[Dict]): Data to load
            if_exists (str): How to behave if the table exists
                             Options: 'fail', 'replace', 'append'
            chunk_size (int): Records per chunk when loading data
            
        Returns:
            int: Number of records inserted
        """
        if isinstance(data, pd.DataFrame):
            records = data.to_dict('records')
        else:
            records = data
            
        if not records:
            self.logger.warning("No data to load")
            return 0
            
        # Get columns from the first record
        columns = list(records[0].keys())
        
        if if_exists == 'replace':
            self.execute_query(f"TRUNCATE TABLE {self.schema}.{table_name}", commit=True)
            self.logger.info(f"Truncated table {table_name}")
            
        # Create placeholders for values
        values_placeholder = ', '.join(['%s'] * len(columns))
        
        insert_query = f"""
        INSERT INTO {self.schema}.{table_name} ({', '.join(columns)})
        VALUES %s
        """
        
        self.logger.info(f"The build query is:\n{insert_query}")
        
        # Process in chunks
        total_inserted = 0
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i+chunk_size]
            values = [[record.get(col) for col in columns] for record in chunk]
            
            try:
                execute_values(self.cursor, insert_query, values)
                self.conn.commit()
                total_inserted += len(chunk)
                self.logger.info(f"Inserted chunk {i//chunk_size + 1} with {len(chunk)} records")
            except Exception as e:
                self.logger.error(f"Error inserting chunk {i//chunk_size + 1}: {e}")
                self.conn.rollback()
                raise
                
        self.logger.info(f"Total {total_inserted} records inserted into {table_name}")
        
        return total_inserted
    
    
    def download_data(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Download data from PostgreSQL to a DataFrame
        
        Args:
            query (str): SQL query to execute
            params (tuple): Parameters for the query
            
        Returns:
            DataFrame: Query results as a pandas DataFrame
        """
        try:
            # Get dictionary cursor for better DataFrame conversion
            dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            dict_cursor.execute(query, params)
            results = dict_cursor.fetchall()
            df = pd.DataFrame(results)
            self.logger.info(f"Downloaded {len(df)} rows from database")
            return df
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            raise
    
    
    def download_table(self, table_name: str, columns: List[str] = None, 
                    condition: str = None, params: tuple = None,
                    limit: int = None, offset: int = None) -> pd.DataFrame:
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
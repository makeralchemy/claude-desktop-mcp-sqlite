import asyncio
import sqlite3
import logging
import os
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from pydantic import BaseModel
from typing import Any, Sequence

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp_sqlite.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
DB_PATH = r"C:\Users\your-user-name\Databases\sqlite-database.db"

app = Server("sqlite-mcp")

# Define Pydantic models for tool arguments
class ListTablesArgs(BaseModel):
    """Arguments for listing tables - no arguments needed"""
    pass

class DescribeTableArgs(BaseModel):
    """Arguments for describing a table"""
    table_name: str

class RunQueryArgs(BaseModel):
    """Arguments for running a SQL query"""
    query: str

@app.list_tools()
async def list_tools() -> list[Tool]:
    logger.info("Tools requested")
    return [
        Tool(
            name="list_tables",
            description="List all tables in the database",
            inputSchema=ListTablesArgs.model_json_schema()
        ),
        Tool(
            name="describe_table", 
            description="Get table structure/schema",
            inputSchema=DescribeTableArgs.model_json_schema()
        ),
        Tool(
            name="run_query",
            description="Execute SQL query",
            inputSchema=RunQueryArgs.model_json_schema()
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> Sequence[TextContent]:
    logger.info(f"Tool called: {name} with arguments: {arguments}")
    
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found: {DB_PATH}")
        return [TextContent(type="text", text=f"Error: Database file not found at {DB_PATH}")]
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if name == "list_tables":
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            result = "Tables in database:\n" + "\n".join(f"- {name}" for name in table_names)
            
        elif name == "describe_table":
            table_name = arguments.get("table_name")
            if not table_name:
                return [TextContent(type="text", text="Error: table_name is required")]
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            if not columns:
                result = f"Table '{table_name}' not found"
            else:
                result = f"Schema for table '{table_name}':\n"
                for col in columns:
                    cid, name, type_, notnull, default, pk = col
                    result += f"- {name}: {type_}"
                    if pk:
                        result += " (PRIMARY KEY)"
                    if notnull:
                        result += " NOT NULL"
                    if default is not None:
                        result += f" DEFAULT {default}"
                    result += "\n"
                    
        elif name == "run_query":
            query = arguments.get("query")
            if not query:
                return [TextContent(type="text", text="Error: query is required")]
            
            cursor.execute(query)
            if query.strip().lower().startswith(('select', 'pragma')):
                rows = cursor.fetchall()
                if rows:
                    # Get column names
                    columns = [description[0] for description in cursor.description]
                    result = "Query results:\n"
                    result += " | ".join(columns) + "\n"
                    result += "-" * (len(" | ".join(columns))) + "\n"
                    for row in rows:
                        result += " | ".join(str(cell) for cell in row) + "\n"
                else:
                    result = "Query executed successfully. No results returned."
            else:
                conn.commit()
                result = f"Query executed successfully. {cursor.rowcount} rows affected."
        
        else:
            result = f"Unknown tool: {name}"
            
        conn.close()
        logger.info(f"Tool {name} executed successfully")
        return [TextContent(type="text", text=result)]
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return [TextContent(type="text", text=f"SQLite error: {e}")]
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return [TextContent(type="text", text=f"Error: {e}")]

async def main():
    logger.info("Starting MCP SQLite server")
    logger.info(f"Database path: {DB_PATH}")
    logger.info(f"Database exists: {os.path.exists(DB_PATH)}")
    
    async with stdio_server() as (read_stream, write_stream):
        logger.info("Server started, waiting for connections")
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())

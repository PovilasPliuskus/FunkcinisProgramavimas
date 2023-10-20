{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement,
    listTables,
  )
where

import DataFrame (Column (..), ColumnType (StringType), Value (StringValue), DataFrame (DataFrame))
import InMemoryTables (TableName, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ParsedStatement
  | ShowTables
  | ShowTable TableName  -- New constructor to specify the table name

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement "SHOW TABLES" = Right ShowTables
parseStatement stmt
  | isShowTableStatement stmt = Right $ ShowTable (extractTableName stmt)
  | otherwise = Left "Not implemented: parseStatement"

-- Helper function to check if the statement is "SHOW TABLE <table-name>"
isShowTableStatement :: String -> Bool
isShowTableStatement stmt = take 11 stmt == "SHOW TABLE "

-- Helper function to extract the table name from "SHOW TABLE <table-name>"
extractTableName :: String -> TableName
extractTableName stmt = drop 11 stmt

-- Executes a parsed statement. Produces a list of table names or columns.
executeStatement :: ParsedStatement -> Either ErrorMessage [String]
executeStatement ShowTables = Right $ listTables InMemoryTables.database
executeStatement (ShowTable tableName) = Right $ listColumns tableName InMemoryTables.database
executeStatement _ = Left "Not implemented: executeStatement"

-- Function to list columns in a table
listColumns :: TableName -> Database -> [String]
listColumns tableName db =
  case lookup tableName db of
    Just (DataFrame columns _) -> map (\(Column colName _) -> colName) columns
    Nothing -> []

parseAndExecute :: String -> Either ErrorMessage [String]
parseAndExecute statement = do
  parsed <- parseStatement statement
  executeStatement parsed

listTables :: Database -> [TableName]
listTables db = map (\(name, _) -> name) db

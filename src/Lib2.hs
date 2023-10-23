{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement,
    listTables,
  )
where

import Data.Char (isSpace, toUpper)
import Data.List (isInfixOf, isPrefixOf, stripPrefix)
import DataFrame (Column (..), ColumnType (StringType), DataFrame (DataFrame), Value (StringValue))
import GHC.RTS.Flags (DebugFlags (stm))
import InMemoryTables (TableName, database)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ParsedStatement
  | ShowTables
  | ShowTable TableName -- New constructor to specify the table name
  --            {- Columns -}  {- Table Name -}  {- MIN Exists -}  {- MIN Column -}  {- AVG Exists -}  {- AVG Column -} {- WhereAND Exists-} {- Where AND stmts -} {- Where Bool is Something exists -} {- Where Bool is Something Column -}
  | Select [String] String Bool String Bool String Bool (ParsedStatement, ParsedStatement) Bool String
  | ColumnList [String] String
  | Min [String] String Bool
  deriving (Show, Eq)

-- \| BoolMin
--  | Min Column
--  | BoolAvg
--  | Avg Column
--  | BoolWhereAND
--  | WhereAND (ParsedStatement, ParsedStatement)
--  | BoolWhereBoolIsSomething
--  | WhereBoolIsSomething Column

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement "SHOW TABLES" = Right ShowTables
parseStatement stmt
  | isShowTableStatement stmt = Right $ ShowTable (extractTableName stmt)
  | otherwise = do
      rest <- startsWithSelect stmt
      case stripPrefix "* FROM " (dropWhile isSpace rest) of
        Just tableName -> Right $ Min (map removeMinPrefix $ listColumns tableName InMemoryTables.database) tableName (containsMin stmt)
        Nothing ->
          let (cols, remaining) = break (== "FROM") (words rest)
           in case remaining of
                ("FROM" : tableNameWords) -> Right $ Min (map removeMinPrefix cols) (unwords tableNameWords) (containsMin stmt)
                _ -> case extractMinColumn stmt of
                  Just columnName -> Right $ Min [removeMinPrefix columnName] columnName (containsMin stmt)
                  Nothing -> Left "Invalid statement: 'FROM' keyword not found."

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
executeStatement (Min columns tableName bool) = Right $ printList columns ++ printBool bool ++ printTableName tableName
-- executeStatement (ColumnList columns tableName) = Right $ printList columns
executeStatement _ = Left "Not implemented: executeStatement"

-- helper function for debugging
printList :: [String] -> [String]
printList [] = [] -- Base case: an empty list, return an empty list
printList (x : xs) = x : printList xs

printTableName :: String -> [String]
printTableName str = [str, str]

printBool :: Bool -> [String]
printBool True = ["True", "True"]
printBool False = ["False", "False"]

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

startsWithSelect :: String -> Either ErrorMessage String
startsWithSelect str
  | "SELECT" `isPrefixOf` map toUpper (dropWhile isSpace str) = Right (drop 6 (dropWhile isSpace str))
  | otherwise = Left "Invalid SELECT format"

containsMin :: String -> Bool
containsMin input = "MIN" `isInfixOf` (map toUpper input)

extractMinColumn :: String -> Maybe String
extractMinColumn str = case splitAt 4 str of
  ("min(", rest) -> case break (== ')') rest of
    (colName, "") -> Just colName
    _ -> Nothing
  _ -> Nothing

removeMinPrefix :: String -> String
removeMinPrefix str = case splitAt 4 str of
  ("min(", colName) -> case reverse colName of
    (')' : rest) -> reverse rest
    _ -> colName
  _ -> str

-- case stripPrefix "* FROM " (dropWhile isSpace stmt) of
--   Just tableName -> tableName
--   Nothing -> ""

-- startsWithAsterisk :: String -> String
-- startsWithAsterisk str
--   | "*" `isPrefixOf` (dropWhile isSpace str) = drop 1 (dropWhile isSpace str)
--   | otherwise = str

-- -- Extract the table name from a SELECT statement
-- extractTableNameFromSelect :: String -> (TableName, String)
-- extractTableNameFromSelect stmt
--   | "FROM" `isInfixOf` stmt =
--       let (beforeFrom, afterFrom) = break (== 'F') stmt
--        in (reverse $ dropWhile isSpace $ reverse beforeFrom, drop 4 afterFrom) -- Drop "FROM"
--   | otherwise = ("", stmt)
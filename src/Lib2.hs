{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement,
    listTables,
  )
where

import Data.Char (isSpace, toUpper)
import Data.List (isInfixOf, isPrefixOf, stripPrefix, tails)
import Data.Maybe
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
  | Min [String] String Bool String Bool String Bool
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
      let (beforeFrom, afterFrom) = span (/= "FROM") (words rest)
      case afterFrom of
        "FROM" : tableNameWords -> do
          let tableName = unwords tableNameWords
          let containsMinKeyword = containsMin stmt
          let containsAvgKeyword = containsAvg stmt
          let containsWhereAndKeyword = containsWhereAnd stmt
          let minColName = case extractMinColumnName stmt of
                Just colName -> colName
                Nothing -> ""
          let avgColName = case extractAvgColumnName stmt of
                Just colName -> colName
                Nothing -> ""
          Right $ Min (map removeMinOrAvgPrefix beforeFrom) (head tableNameWords) containsMinKeyword minColName containsAvgKeyword avgColName containsWhereAndKeyword
        _ -> Left "Invalid statement: 'FROM' keyword not found."

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
executeStatement (Min columns tableName boolMin minColName boolAvg avgColName boolWhereAnd) = Right $ printList columns ++ printBool boolMin ++ printTableName tableName ++ [minColName] ++ printBool boolAvg ++ [avgColName] ++ printBool boolWhereAnd
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

containsAvg :: String -> Bool
containsAvg input = "AVG" `isInfixOf` (map toUpper input)

containsWhereAnd :: String -> Bool
containsWhereAnd input = "WHERE" `isInfixOf` (map toUpper input)

extractMinColumn :: String -> Maybe String
extractMinColumn str = case splitAt 4 str of
  ("min(", rest) -> case break (== ')') rest of
    (colName, "") -> Just colName
    _ -> Nothing
  _ -> Nothing

removeMinOrAvgPrefix :: String -> String
removeMinOrAvgPrefix str = case str of
  ('m' : 'i' : 'n' : '(' : rest) -> extractColumnName rest
  ('a' : 'v' : 'g' : '(' : rest) -> extractColumnName rest
  _ -> str
  where
    extractColumnName = takeWhile (/= ')')

extractMinColumnName :: String -> Maybe String
extractMinColumnName input = listToMaybe [cleanWord word | word <- words input, isMinColumn word]

isMinColumn :: String -> Bool
isMinColumn word = "min(" `isPrefixOf` word

extractAvgColumnName :: String -> Maybe String
extractAvgColumnName input = listToMaybe [cleanWord word | word <- words input, isAvgColumn word]

isAvgColumn :: String -> Bool
isAvgColumn word = "avg(" `isPrefixOf` word

cleanWord :: String -> String
cleanWord word = reverse $ drop 1 $ reverse $ drop 4 word
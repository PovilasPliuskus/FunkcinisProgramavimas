{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

-- isPrefixOf used in the second exercise

import Data.Char (toLower)
import Data.List (isPrefixOf)
import DataFrame (ColumnType (BoolType), DataFrame)
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement

-- checks if the format is correct
isValid :: String -> Bool
isValid command = "select * from " `isPrefixOf` map toLower command

-- removes ';' at the end (if needed) and returns the TableName
extractTableName :: String -> TableName
extractTableName command
  | lastChar == ';' = drop 14 (init command)
  | otherwise = drop 14 command
  where
    lastChar = last command

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement command
  | null command = Left "Input is empty"
  | isValid command = Right (extractTableName command)
  | otherwise = Left "Invalid SELECT format"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame _ = error "validateDataFrame ot implemented"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"

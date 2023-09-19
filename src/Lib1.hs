{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.List (transpose)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))
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
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..

-- checks if one column match value types
checkColumnsValueTypes :: Column -> [Value] -> Bool
checkColumnsValueTypes (Column _ expectedType) values =
  all
    ( \value ->
        case (expectedType, value) of
          (IntegerType, IntegerValue _) -> True
          (StringType, StringValue _) -> True
          (BoolType, BoolValue _) -> True
          (_, NullValue) -> True
          _ -> False
    )
    values

-- checks if All the columns match value types
checkAllColumnValueTypes :: DataFrame -> Bool
checkAllColumnValueTypes (DataFrame columns values) =
  all (\(column, columnValues) -> checkColumnsValueTypes column columnValues) (zip columns (transpose values))

validateRows :: DataFrame -> Either ErrorMessage ()
validateRows (DataFrame columns rows) =
  if all (\row -> length row == length columns) rows
    then Right ()
    else Left "Row sizes do not match columns"

validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame df@(DataFrame _ _) =
  case (checkAllColumnValueTypes df, validateRows df) of
    (True, Right ()) -> Right ()
    (_, Left errMsg) -> Left errMsg
    (False, _) -> Left "Columns have mismatched types"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"

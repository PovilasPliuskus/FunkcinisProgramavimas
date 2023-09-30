

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

-- isPrefixOf used in the second exercise

import Data.Bits (Bits (xor))
import Data.Char
import Data.List
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import GHC.OldList (intercalate)
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName ((tableName, dataFrame) : database) givenName
  | map toLower tableName == map toLower givenName = Just dataFrame
  | otherwise = findTableByName database givenName

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement

-- checks if the format is correct
isValid :: String -> Bool
isValid command = "select * from " `isPrefixOf` map toLower command

-- removes ';' at the end (if needed) and returns the TableName
extractTableName :: String -> TableName
extractTableName command
  | lastChar == ';' = dropWhile isSpace $ drop 14 (init command)
  | otherwise = dropWhile isSpace $ drop 14 command
  where
    lastChar = last command

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement command
  | null command = Left "Input is empty"
  | isValid command = Right (extractTableName command)
  | otherwise = Left "Invalid SELECT format"

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
-- Example usage in renderDataFrameAsTable:

renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns values) =
  let numColumns = calculateNumberOfColumns columns
      cappedWidth = min 100 terminalWidth
      columnWidth = calculateOneColumnWidth cappedWidth numColumns
      headerRow = generateHeaderRow columns columnWidth
      dataRows = map (generateDataRow columns columnWidth) values
   in unlines (headerRow : dataRows)

calculateNumberOfColumns :: [Column] -> Integer
calculateNumberOfColumns columns =
  let numColumns = length columns
   in fromIntegral numColumns

calculateOneColumnWidth :: Integer -> Integer -> Integer
calculateOneColumnWidth terminalWidth numColumns =
  terminalWidth `div` numColumns

generateHeaderRow :: [Column] -> Integer -> String
generateHeaderRow columns columnWidth =
  let columnNames = map (\(Column name _) -> name) columns
      formattedColumnNames = intercalate " | " (map (`formatColumn` columnWidth) columnNames)
      separatorLine = intercalate "-+-" (replicate (length columnNames) (replicate (fromIntegral columnWidth) '-'))
   in formattedColumnNames ++ "\n" ++ separatorLine

formatColumn :: String -> Integer -> String
formatColumn columnName width =
  let padding = max 0 (width - fromIntegral (length columnName))
   in columnName ++ replicate (fromIntegral padding) ' '

generateDataRow :: [Column] -> Integer -> Row -> String
generateDataRow columns columnWidth row =
  let formattedValues = map (`formatColumn` columnWidth) (map valueToString row)
   in intercalate " | " formattedValues

valueToString :: Value -> String
valueToString (IntegerValue x) = show x
valueToString (StringValue x) = x
valueToString (BoolValue x) = show x
valueToString NullValue = "NULL"

-- ColumnName1 | ColumnName2 | ColumnName3
-- ------------+-------------+------------
-- Row11       | Row21       | Row31
-- Row12       | Row22       | Row32
-- Row13       | Row23       | Row33
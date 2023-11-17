{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement,
    listTables,
  )
where

import Data.Char (isSpace, toLower, toUpper)
import Data.List
import Data.List (isInfixOf, isPrefixOf, stripPrefix, tails)
import Data.Maybe
import Data.Ord (comparing)
import DataFrame (Column (..), ColumnType (IntegerType, StringType), DataFrame (DataFrame), Row (..), Value (..))
import GHC.RTS.Flags (DebugFlags (stm))
import InMemoryTables (TableName, database)
import Lib1 (renderDataFrameAsTable)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ParsedStatement
  | ShowTables
  | ShowTable TableName -- New constructor to specify the table name
  --            {- Columns -}  {- Table Name -}  {- MIN Exists -}  {- MIN Column -}  {- AVG Exists -}  {- AVG Column -} {- WhereAND Exists-} {- Where AND stmts -} {- Where Bool is Something exists -} {- Where Bool is Something Column -}
  | ColumnList [String] String
  | Select [String] String Bool String Bool String Bool String String Bool String
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
parseStatement stmt
  | isShowTablesStatement stmt = Right $ ShowTables
  | isShowTableStatement stmt = Right $ ShowTable (extractTableName stmt)
  | otherwise = do
      rest <- startsWithSelect stmt
      let (beforeFrom, afterFrom) = span (/= "from") (words (map toLower rest))
      case afterFrom of
        "from" : tableNameWords -> do
          let containsMinKeyword = containsMin stmt
          let containsAvgKeyword = containsAvg stmt
          let containsWhereAndKeyword = containsWhereAnd stmt
          let containsWhereBoolKeyword = containsWhereBool stmt
          let minColName = case extractMinColumnName stmt of
                Just colName -> colName
                Nothing -> ""
          let avgColName = case extractAvgColumnName stmt of
                Just colName -> colName
                Nothing -> ""
          let columnList = map removeCommas $ map removeMinOrAvgPrefix beforeFrom
          let condition1 = case extractFirstCondition stmt of
                Just con1 -> con1
                Nothing -> ""
          let condition2 = case extractSecondCondition stmt of
                Just con2 -> con2
                Nothing -> ""
          let whereBoolCondition = case extractWhereBoolCondition stmt of
                Just col -> col
                Nothing -> ""
          Right $ Select columnList (head tableNameWords) containsMinKeyword minColName containsAvgKeyword avgColName containsWhereAndKeyword condition1 condition2 containsWhereBoolKeyword whereBoolCondition
        _ -> Left "Invalid statement: 'FROM' keyword not found."

removeCommas :: String -> String
removeCommas = filter (/= ',')

-- Helper function to check if the statement is "SHOW TABLE <table-name>"
isShowTableStatement :: String -> Bool
isShowTableStatement stmt = take 11 (map toLower stmt) == "show table "

isShowTablesStatement :: String -> Bool
isShowTablesStatement stmt = map toLower stmt == "show tables"

-- Helper function to extract the table name from "SHOW TABLE <table-name>"
extractTableName :: String -> TableName
extractTableName stmt = drop 11 stmt

-- Executes a parsed statement. Produces a list of table names or columns.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = do
  let tableNames = listTables InMemoryTables.database
  let columns = [Column "Table Name" StringType]
  let rows = map (\name -> [StringValue name]) tableNames
  Right $ DataFrame columns rows
executeStatement (ShowTable tableName) = Right $ listColumns tableName InMemoryTables.database
-- executeStatement (ShowTable tableName) =
--   case findTableByName InMemoryTables.database tableName of
--     Just table -> Right table
--     Nothing -> Left $ "Table not found: " ++ tableName
executeStatement (Select columns tableName boolMin minColName boolAvg avgColName boolWhereAnd con1 con2 boolWhereBool whereBool) = do
  let table = findTableByName InMemoryTables.database tableName
  let columnsToRender = if "*" `elem` columns then calculateAllColumns table else columns
  if boolMin
    then do
      if length columnsToRender /= 1
        then Left "Only one column can be selected when using function MIN"
        else do
          let minResult = extractMinValueFromColumn tableName minColName
          let minTableString = DataFrame [Column ("min(" ++ minColName ++ ")") StringType] [[minResult]]
          case minResult of
            IntegerValue intValue -> do
              let minTable = DataFrame [Column ("min(" ++ minColName ++ ")") IntegerType] [[minResult]]
              Right minTable
            _ -> Right minTableString
    else
      if boolAvg
        then do
          if length columnsToRender /= 1
            then Left "Only one column can be selected when using function AVG"
            else do
              let avgValue = calculateAverage tableName avgColName
              let avgTable = DataFrame [Column ("avg(" ++ avgColName ++ ")") StringType] [[StringValue (show avgValue)]]
              Right avgTable
        else
          if boolWhereBool
            then do
              let filteredTable = buildWhereBoolDataFrame whereBool table
              Right filteredTable
            else
              if boolWhereAnd
                then do
                  let (column1, value1) = extractConditionParts con1
                  let (column2, value2) = extractConditionParts con2
                  if con1 == con2
                    then do
                      let filteredTable = filterTableWithSingleCondition column1 value1 table
                      Right filteredTable
                    else do
                      let filteredTable = filterTableWithWhereAnd column1 value1 column2 value2 table
                      Right filteredTable
                else Right $ selectColumns table columnsToRender
executeStatement _ = Left "Not implemented: executeStatement"

filterTableWithSingleCondition :: String -> String -> DataFrame -> DataFrame
filterTableWithSingleCondition column value table =
  filterTableWithWhereAnd column value column value table

intersectTables :: DataFrame -> DataFrame -> DataFrame
intersectTables (DataFrame cols1 rows1) (DataFrame cols2 rows2) =
  let commonCols = intersectBy (\(Column name1 _) (Column name2 _) -> name1 == name2) cols1 cols2
      colIndices1 = map (colNameToIndex cols1) commonCols
      colIndices2 = map (colNameToIndex cols2) commonCols
      filteredRows1 = map (selectRowIndices colIndices1) rows1
      filteredRows2 = map (selectRowIndices colIndices2) rows2
      commonRows = filter (`elem` filteredRows2) filteredRows1
   in DataFrame commonCols commonRows

colNameToIndex :: [Column] -> Column -> Int
colNameToIndex cols col = fromMaybe 0 (elemIndex col cols)

selectRowIndices :: [Int] -> Row -> Row
selectRowIndices indices row = [row !! index | index <- indices]

-- Helper function to extract column name and value from condition
extractConditionParts :: String -> (String, String)
extractConditionParts condition =
  case words condition of
    [columnName, "=", value] -> (columnName, value)
    _ -> ("", "") -- Handle the case where the condition doesn't match the expected format

-- Helper function to filter table using WHERE AND condition
filterTableWithWhereAnd :: String -> String -> String -> String -> DataFrame -> DataFrame
filterTableWithWhereAnd column1 value1 column2 value2 table =
  let columnIndex1 = getColumnIndex table column1
      columnIndex2 = getColumnIndex table column2
      filteredRows = filter (\row -> checkCondition row columnIndex1 value1 && checkCondition row columnIndex2 value2) (dataRows table)
   in DataFrame (columns table) filteredRows
  where
    checkCondition :: Row -> Maybe Int -> String -> Bool
    checkCondition row (Just columnIndex) value =
      case row !! columnIndex of
        StringValue str -> str == value
        IntegerValue int -> show int == value
        BoolValue bool -> show bool == value
        _ -> False
    checkCondition _ _ _ = False

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

dataRows :: DataFrame -> [[Value]]
dataRows (DataFrame _ rows) = rows

calculateAllColumns :: DataFrame -> [String]
calculateAllColumns (DataFrame columns _) =
  map (\(Column name _) -> name) columns

selectColumns :: DataFrame -> [String] -> DataFrame
selectColumns (DataFrame columns rows) selectedColumns =
  let columnIndexMap = mapColumnIndex columns
      selectedColumnIndices = mapMaybe (`lookup` columnIndexMap) selectedColumns
      selectedColumns' = map (\i -> columns !! i) selectedColumnIndices
      selectedRows = map (selectRow selectedColumnIndices) rows
   in DataFrame selectedColumns' selectedRows

mapColumnIndex :: [Column] -> [(String, Int)]
mapColumnIndex columns = zip (map (\(Column name _) -> name) columns) [0 ..]

selectRow :: [Int] -> Row -> Row
selectRow selectedIndices row = map (\i -> row !! i) selectedIndices

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
listColumns :: TableName -> Database -> DataFrame
listColumns tableName db =
  case lookup tableName db of
    Just (DataFrame columns _) ->
      let columnNames = map (\(Column colName _) -> [StringValue colName]) columns
       in DataFrame [Column "Tables" StringType] columnNames
    Nothing -> DataFrame [Column "Tables" StringType] []

-- parseAndExecute :: String -> Either ErrorMessage [String]
-- parseAndExecute statement = do
--   parsed <- parseStatement statement
--   executeStatement parsed

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
containsWhereAnd input =
  ("AND" `isInfixOf` (map toUpper input)) || ("WHERE" `isInfixOf` (map toUpper input))

containsWhereBool :: String -> Bool
containsWhereBool input =
  ("FALSE" `isInfixOf` (map toUpper input) && "WHERE" `isInfixOf` (map toUpper input))
    || ("TRUE" `isInfixOf` (map toUpper input) && "WHERE" `isInfixOf` (map toUpper input))

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

extractFirstCondition :: String -> Maybe String
extractFirstCondition input =
  case dropWhile (/= "WHERE") (words input) of
    ("WHERE" : rest) ->
      case break (== "AND") rest of
        (conditions, _) -> Just (unwords conditions)
    _ -> Nothing

extractSecondCondition :: String -> Maybe String
extractSecondCondition input =
  let wordsList = words input
   in case "AND" `elem` wordsList of
        True -> case dropWhile (/= "AND") wordsList of
          ("AND" : rest) -> Just (unwords rest)
          _ -> Nothing
        False -> case dropWhile (/= "WHERE") wordsList of
          ("WHERE" : rest) ->
            case break (== "AND") rest of
              (conditions, _) -> Just (unwords conditions)
          _ -> Nothing

extractWhereBoolCondition :: String -> Maybe String
extractWhereBoolCondition input =
  case findIndex (isPrefixOf "WHERE") (words input) of
    Just index -> Just $ unwords (drop (index + 1) (words input))
    Nothing -> Nothing

extractMinValueFromColumn :: TableName -> String -> Value
extractMinValueFromColumn tableName columnName =
  let dataFrame = findTableByName InMemoryTables.database tableName
   in case getColumnIndex dataFrame columnName of
        Just columnIndex ->
          let values = getColumnValues dataFrame columnIndex
              filteredValues = filterValues values
           in case getMinimumValue filteredValues of
                Just minVal -> minVal
                Nothing -> NullValue
        Nothing -> NullValue

getMinimumValue :: [Value] -> Maybe Value
getMinimumValue [] = Nothing
getMinimumValue (x : xs) = Just (findMinimum x xs)

findMinimum :: Value -> [Value] -> Value
findMinimum currentMin [] = currentMin
findMinimum currentMin (x : xs)
  | x `isLessThan` currentMin = findMinimum x xs
  | otherwise = findMinimum currentMin xs

isLessThan :: Value -> Value -> Bool
isLessThan (IntegerValue x) (IntegerValue y) = x < y
isLessThan (IntegerValue x) (StringValue y) = show x < y
isLessThan (StringValue x) (IntegerValue y) = x < show y
isLessThan (StringValue x) (StringValue y) = x < y
isLessThan _ _ = False

getValueForComparison :: Value -> (String, Integer, Bool)
getValueForComparison (StringValue str) = (str, 0, False)
getValueForComparison (IntegerValue int) = ("", int, False)
getValueForComparison (BoolValue bool) = ("", 0, bool)
getValueForComparison _ = ("", 0, False)

findTableByName :: Database -> String -> DataFrame
findTableByName ((tableName, dataFrame) : database) givenName
  | map toLower tableName == map toLower givenName = dataFrame
  | otherwise = findTableByName database givenName

getColumnIndex :: DataFrame -> String -> Maybe Int
getColumnIndex (DataFrame columns _) columnName =
  elemIndex columnName (map (\(Column name _) -> name) columns)

getColumnValues :: DataFrame -> Int -> [Value]
getColumnValues (DataFrame _ rows) columnIndex =
  map (!! columnIndex) rows

filterValues :: [Value] -> [Value]
filterValues values =
  filter (\v -> v /= NullValue) values

calculateAverage :: TableName -> String -> Double
calculateAverage tableName columnName =
  let dataFrame = findTableByName InMemoryTables.database tableName
   in case getColumnIndex dataFrame columnName of
        Just columnIndex ->
          let values = getColumnValues dataFrame columnIndex
              numericValues = filterNumericValues values
           in case numericValues of
                [] -> 0.0 -- Return 0.0 if no numeric values are found
                _ -> sumNumericValues numericValues / fromIntegral (length numericValues)
        Nothing -> 0.0 -- Return 0.0 if the column doesn't exist

filterNumericValues :: [Value] -> [Double]
filterNumericValues values =
  [toDouble val | IntegerValue val <- values]

toDouble :: Integer -> Double
toDouble = fromIntegral

sumNumericValues :: [Double] -> Double
sumNumericValues = foldl' (+) 0.0

-- renderDataFrameAsTable :: Integer -> DataFrame -> String
-- renderDataFrameAsTable terminalWidth (DataFrame columns values) =
--   let numColumns = calculateNumberOfColumns columns
--       cappedWidth = min 100 terminalWidth
--       columnWidth = calculateOneColumnWidth cappedWidth numColumns
--       headerRow = generateHeaderRow columns columnWidth
--       dataRows = map (generateDataRow columns columnWidth) values
--    in unlines (headerRow : dataRows)

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

buildWhereBoolDataFrame :: String -> DataFrame -> DataFrame
buildWhereBoolDataFrame condition (DataFrame columns rows) =
  let (colName, expectedValue) = parseWhereBoolCondition condition
      indexMap = mapColumnIndex columns
      colIndex = fromMaybe (-1) (lookup colName indexMap)
      filteredRows = filterRowsWithBoolCondition colIndex expectedValue rows
   in DataFrame columns filteredRows

checkCondition :: (EqValue a) => Row -> Int -> Value -> a -> Bool
checkCondition row colIndex value expectedValue =
  case row `at` colIndex of
    BoolValue b -> checkEqual (BoolValue b) expectedValue
    _ -> False

class (Eq a) => EqValue a where
  checkEqual :: Value -> a -> Bool

instance EqValue Bool where
  -- checkEqual :: Value -> Bool -> Bool
  checkEqual (BoolValue value) expectedValue = value == expectedValue
  checkEqual _ _ = False

at :: [a] -> Int -> a
at xs i
  | i >= 0 && i < length xs = xs !! i
  | otherwise = error "Index out of bounds"

parseWhereBoolCondition :: String -> (String, Bool)
parseWhereBoolCondition condition =
  case words condition of
    [columnName, "=", "TRUE"] -> (columnName, True)
    [columnName, "=", "FALSE"] -> (columnName, False)
    _ -> error "Invalid condition format"

filterRowsWithBoolCondition :: (EqValue a) => Int -> a -> [Row] -> [Row]
filterRowsWithBoolCondition colIndex expectedValue rows =
  filter (\row -> checkCondition row colIndex (row `at` colIndex) expectedValue) rows

selectColumnsByIndex :: DataFrame -> [Int] -> [Column]
selectColumnsByIndex (DataFrame columns _) indices =
  [columns !! i | i <- indices]

getDataRows :: DataFrame -> [Row]
getDataRows (DataFrame _ rows) = rows

printResult :: Either ErrorMessage [String] -> IO ()
printResult result = case result of
  Right strings -> mapM_ putStrLn strings
  Left errorMessage -> putStrLn errorMessage
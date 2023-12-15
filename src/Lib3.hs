{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Control.Exception (IOException, try)
import Control.Monad (foldM)
import Control.Monad.Free (Free (..), liftF)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON, eitherDecode)
import Data.ByteString.Char8 qualified as BS
import Data.ByteString.Lazy.Char8 qualified as BLC
import Data.Char (isSpace, toLower, toUpper)
import Data.List
import Data.List (isInfixOf, isPrefixOf, stripPrefix, tails)
import Data.Maybe
import Data.Ord (comparing)
import Data.Text qualified as T
import Data.Time (TimeZone (..), UTCTime (..), defaultTimeLocale, getCurrentTime, utc)
import Data.Time.Clock (UTCTime, addUTCTime, nominalDiffTimeToSeconds)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Time.Format (formatTime)
import Data.Yaml (FromJSON, ToJSON, decodeFileEither, encode)
import Data.Yaml qualified as Y
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row (..), Value (..))
import GHC.Generics
import GHC.RTS.Flags (DebugFlags (stm))
import InMemoryTables (TableName, database)
import System.IO.Unsafe (unsafePerformIO)

type Condition = String

type ContainsWhere = Bool

type FileContent = String

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

type ColumnName = String

type InsertValues = String

type UpdateValues = String

data ParsedStatement
  = ParsedStatement
  | Select [ColumnName] [TableName] ContainsWhere Condition
  | Insert TableName [ColumnName] [InsertValues]
  | Update TableName [(ColumnName, UpdateValues)] Condition
  | Delete TableName Condition
  deriving (Show, Eq)

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving (Functor)

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql
  | isNowStatement sql = do
      currentTime <- getTime
      return $ Right (createNowDataFrame currentTime)
  | containsInsert sql = do
      case insertParser sql of
        Right (Insert tableName columns values) -> do
          df <- readContentFromYAML tableName
          case df of
            Right existingDataFrame -> do
              let updatedDataFrame = insertToDataFrame (Insert tableName columns values) existingDataFrame
              return updatedDataFrame
            Left errorMessage -> return $ Left errorMessage
        Left errorMessage -> return $ Left errorMessage
  | containsDelete sql = do
      case deleteParser sql of
        Right (Delete tableName condition) -> do
          df <- readContentFromYAML tableName
          case df of
            Right existingDataFrame -> do
              let updatedDataFrame = deleteFromDataFrame (Delete tableName condition) existingDataFrame
              return updatedDataFrame
            Left errorMessage -> return $ Left errorMessage
        Left errorMessage -> return $ Left errorMessage
  | containsUpdate sql = do
      case updateParser sql of
        Right (Update tableName updates condition) -> do
          df <- readContentFromYAML tableName
          case df of
            Right existingDataFrame -> do
              let updatedDataFrame = updateDataFrame (Update tableName updates condition) existingDataFrame
              return updatedDataFrame
            Left errorMessage -> return $ Left errorMessage
        Left errorMessage -> return $ Left errorMessage
  | otherwise = do
      case parseSelect sql of
        Right tableName -> do
          let hasWhere = containsWhere sql
          let sqlWithoutSelect = removeSelect sql
          case extractColumns sqlWithoutSelect of
            Right columns -> do
              let sqlAfterFrom = removeBeforeFrom sqlWithoutSelect
              case extractTableNames sqlAfterFrom of
                Right tableNames -> do
                  case hasWhere of
                    True -> do
                      let condition = removeTrailingSemicolon (removeBeforeWhere sqlAfterFrom)
                      let (column, value) = extractConditionParts condition
                      case createDataFrameFromFiles (Select columns tableNames hasWhere condition) of
                        Right dataFrame -> do
                          let filteredTable = filterTable column value dataFrame
                          return $ Right filteredTable
                        Left errorMessage -> return $ Left errorMessage
                    False ->
                      case createDataFrameFromFiles (Select columns tableNames hasWhere "") of
                        Right dataFrame -> return $ Right dataFrame
                        Left errorMessage -> return $ Left errorMessage
                Left errorMessage -> return $ Left errorMessage
            Left errorMessage -> return $ Left errorMessage
        Left errorMessage -> return $ Left errorMessage

containsDelete :: String -> Bool
containsDelete input = case words (map toLower input) of
  ("delete" : _) -> True
  _ -> False

containsFrom :: String -> Bool
containsFrom input = case words (map toLower input) of
  ("from" : _) -> True
  _ -> False

containsInsert :: String -> Bool
containsInsert input = case words (map toLower input) of
  ("insert" : _) -> True
  _ -> False

containsUpdate :: String -> Bool
containsUpdate input = case words (map toLower input) of
  ("update" : _) -> True
  _ -> False

containsInto :: String -> Bool
containsInto input = case words (map toLower input) of
  ("into" : _) -> True
  _ -> False

containsValues :: String -> Bool
containsValues input = case words (map toLower input) of
  ("values" : _) -> True
  _ -> False

extractTableName :: String -> Either ErrorMessage TableName
extractTableName str =
  case words str of
    [] -> Left "Error: Please enter a table name"
    (tableName : _) -> Right tableName

containsOpeningBracket :: String -> Bool
containsOpeningBracket input = not (null input) && head input == '('

dropChar :: String -> String
dropChar [] = []
dropChar (_ : xs) = xs

dropWord :: String -> String
dropWord = unwords . drop 1 . words

getSubstringBeforeLastClosingParen :: String -> Either ErrorMessage String
getSubstringBeforeLastClosingParen s =
  case break (== ')') s of
    (before, ')' : after) -> Right (before ++ " from")
    _ -> Left "Error: No closing parenthesis found"

getSubstringAfterLastClosingParen :: String -> String
getSubstringAfterLastClosingParen s = go s ""
  where
    go :: String -> String -> String
    go [] acc = acc
    go (')' : rest) acc = rest
    go (c : rest) acc = go rest (c : acc)

extractColumnNamesUntilClosingParenthesis :: String -> Either ErrorMessage [ColumnName]
extractColumnNamesUntilClosingParenthesis sql = do
  substring <- getSubstringBeforeLastClosingParen sql
  extractColumns substring

-- tableEmployees :: DataFrame
-- tableEmployees =
--   DataFrame
--     [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
--     [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
--       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
--     ]

-- tableWithNulls :: DataFrame
-- tableWithNulls =
--   DataFrame
--     [Column "flag" StringType, Column "value" BoolType]
--     [ [StringValue "a", BoolValue True],
--       [StringValue "b", BoolValue True],
--       [StringValue "b", NullValue],
--       [StringValue "b", BoolValue False]
--     ]

encodeDataFrame :: DataFrame -> TableName -> IO ()
encodeDataFrame df tableName = BS.writeFile ("src/db/" ++ tableName ++ ".yaml") (encode df)

readDataFrameFromJSON :: FilePath -> Either String DataFrame
readDataFrameFromJSON filePath =
  unsafePerformIO $ do
    let fullPath = "src/db/" ++ filePath ++ ".json"
    content <- BLC.readFile fullPath
    return $ eitherDecode content

readDataFrameFromYAML :: FilePath -> Either String DataFrame
readDataFrameFromYAML filePath =
  unsafePerformIO $ do
    let fullPath = "src/db/" ++ filePath ++ ".yaml"
    result <- decodeFileEither fullPath
    return $ case result of
      Right dataFrame -> Right dataFrame
      Left err -> Left $ "Error decoding YAML from file " ++ fullPath ++ ": " ++ show err

readContentFromYAML :: TableName -> Execution (Either String DataFrame)
readContentFromYAML tableName = do
  result <- loadFile tableName
  case Y.decodeEither' (BS.pack result) of
    Left err -> return $ Left $ "Failed to deserialize 'database.yaml': " ++ show err
    Right db -> Pure $ Right db

-- readDataFrameFromJSONPure :: FilePath -> Either String DataFrame
-- readDataFrameFromJSONPure filePath = do
--   let fullPath = "src/db/" ++ filePath ++ ".json"
--   content <- BS.readFile fullPath
--   let lazyContent = BLC.fromStrict content
--   eitherDecode lazyContent

-- printDataFrameFromJSON :: FilePath -> IO ()
-- printDataFrameFromJSON fileName = do
--   let filePath = "src/db/" ++ fileName ++ ".json"
--   eitherDataFrame <- readDataFrameFromJSON filePath
--   case eitherDataFrame of
--     Left err -> putStrLn $ "Error decoding JSON from file " ++ filePath ++ ": " ++ err
--     Right df -> print df

maybeToEither :: a -> Maybe b -> Either a b
maybeToEither err = maybe (Left err) Right

createNowDataFrame :: UTCTime -> DataFrame
createNowDataFrame currentTime =
  let timeZoneOffset = 2
      adjustedTime = addUTCTime (fromIntegral $ timeZoneOffset * 3600) currentTime
      column = Column "Now" StringType
      row = [StringValue (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" adjustedTime)]
   in DataFrame [column] [row]

isNowStatement :: String -> Bool
isNowStatement stmt = map toLower stmt == "now()"

extractConditionParts :: String -> (String, String)
extractConditionParts condition =
  case words condition of
    [columnName, "=", value] -> (map toLower columnName, map toLower value)
    _ -> ("", "")

filterTable :: String -> String -> DataFrame -> DataFrame
filterTable column value table =
  let columnIndex = getColumnIndex table column
      filteredRows = filter (\row -> checkCondition row columnIndex value) (dataRows table)
   in DataFrame (columns table) filteredRows
  where
    checkCondition :: Row -> Maybe Int -> String -> Bool
    checkCondition row (Just columnIndex) value =
      case (row !! columnIndex, value) of
        (StringValue str, _) -> map toLower str == map toLower value
        (IntegerValue int, _) -> show int == value
        (BoolValue bool, "true") -> bool
        (BoolValue bool, "false") -> not bool
        _ -> False
    checkCondition _ _ _ = False

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

dataRows :: DataFrame -> [[DataFrame.Value]]
dataRows (DataFrame _ rows) = rows

getColumnIndex :: DataFrame -> String -> Maybe Int
getColumnIndex (DataFrame columns _) columnName =
  elemIndex columnName (map (\(Column name _) -> name) columns)

parseSelect :: String -> Either ErrorMessage String
parseSelect stmt =
  case map toLower <$> words stmt of
    ("select" : _) -> Right (removeSelect stmt)
    _ -> Left "Error: SELECT statement not found"

removeSelect :: String -> String
removeSelect = unwords . filter (/= "select") . words . map toLower

findTableByName :: Database -> String -> Either ErrorMessage DataFrame
findTableByName [] _ = Left "Error: Table not found"
findTableByName ((tableName, dataFrame) : database) givenName
  | map toLower tableName == map toLower givenName = Right dataFrame
  | otherwise = findTableByName database givenName

extractColumns :: String -> Either ErrorMessage [ColumnName]
extractColumns sql =
  case wordsWhen (\c -> isSpace c || c == ',') sql of
    [] -> Left "Error: No columns found before FROM"
    ws ->
      let columns = map (filter (/= ',')) $ takeWhile (not . isFromKeyword) ws
       in if null columns
            then Left "Error: No columns found before FROM"
            else Right columns
  where
    isFromKeyword :: String -> Bool
    isFromKeyword s = map toLower s == "from"

    wordsWhen :: (Char -> Bool) -> String -> [String]
    wordsWhen p s = case dropWhile p s of
      "" -> []
      s' -> w : wordsWhen p s''
        where
          (w, s'') = break p s'

removeBeforeFrom :: String -> String
removeBeforeFrom = unwords . drop 1 . dropWhile (/= "from") . words . map toLower

removeBeforeWhere :: String -> String
removeBeforeWhere input =
  case break (== "where") (words (map toLower input)) of
    (_, []) -> input
    (_, rest) -> unwords (drop 1 rest)

removeTrailingSemicolon :: String -> String
removeTrailingSemicolon input =
  if last input == ';'
    then init input
    else input

containsWhere :: String -> Bool
containsWhere input = "where" `elem` words (map toLower input)

extractTableNames :: String -> Either ErrorMessage [TableName]
extractTableNames sql =
  case wordsWhen (\c -> isSpace c || c == ',') sql of
    [] -> Left "Error: No table names found"
    ws ->
      let tableNames = map (removeSemicolon . filter (/= ',')) $ takeWhile (not . isSemicolonOrWhereKeyword) ws
       in if null tableNames
            then Left "Error: No table names found"
            else Right tableNames
  where
    isSemicolonOrWhereKeyword :: String -> Bool
    isSemicolonOrWhereKeyword s =
      map toLower (removeSemicolon s) == "where"

    removeSemicolon :: String -> String
    removeSemicolon = filter (/= ';')

    wordsWhen :: (Char -> Bool) -> String -> [String]
    wordsWhen p s = case dropWhile p s of
      "" -> []
      s' -> w : wordsWhen p s''
        where
          (w, s'') = break p s'

-- createDataFrame :: ParsedStatement -> Either ErrorMessage DataFrame
-- createDataFrame (Select columns tableNames whereBool con) = do
--   dataFrames <- mapM (\tableName -> findTableByName InMemoryTables.database tableName) tableNames
--   combinedDataFrame <- combineDataFrames dataFrames
--   result <- selectColumns combinedDataFrame columns
--   return result

createDataFrameFromFiles :: ParsedStatement -> Either ErrorMessage DataFrame
createDataFrameFromFiles (Select columns tableNames whereBool con) = do
  dataFrames <- mapM (\tableName -> readDataFrameFromYAML tableName) tableNames
  combinedDataFrame <- combineDataFrames dataFrames
  result <- selectColumns combinedDataFrame columns
  return result

combineDataFrames :: [DataFrame] -> Either ErrorMessage DataFrame
combineDataFrames [] = Left "Error: No DataFrames to combine"
combineDataFrames (firstDataFrame : restDataFrames) =
  foldM combineTwoDataFrames firstDataFrame restDataFrames

combineTwoDataFrames :: DataFrame -> DataFrame -> Either ErrorMessage DataFrame
combineTwoDataFrames (DataFrame cols1 rows1) (DataFrame cols2 rows2) =
  case (cols1, cols2) of
    ([], _) -> Left "Error: First DataFrame has no columns"
    (_, []) -> Left "Error: Second DataFrame has no columns"
    (_, _) ->
      let combinedColumns = cols1 ++ cols2
          combinedRows = if null rows1 || null rows2 then [] else [row1 ++ row2 | row1 <- rows1, row2 <- rows2]
       in Right (DataFrame combinedColumns combinedRows)

selectColumns :: DataFrame -> [ColumnName] -> Either ErrorMessage DataFrame
selectColumns (DataFrame columns rows) selectedColumns =
  let columnIndexMap = mapColumnIndex columns
      selectedColumnIndices = mapM (`lookup` columnIndexMap) selectedColumns
   in case selectedColumnIndices of
        Just indices ->
          let selectedColumns' = map (\i -> columns !! i) indices
              selectedRows = map (selectRow indices) rows
           in Right (DataFrame selectedColumns' selectedRows)
        Nothing -> Left "Error: One or more columns do not exist in the table"

mapColumnIndex :: [Column] -> [(String, Int)]
mapColumnIndex columns = zip (map (\(Column name _) -> name) columns) [0 ..]

selectRow :: [Int] -> Row -> Row
selectRow indices row = map (\i -> row !! i) indices

helperFunction :: String -> Either ErrorMessage String
helperFunction sql = do
  case parseSelect sql of
    Right tableName -> do
      let sqlWithoutSelect = removeSelect sql
      let hasWhere = containsWhere sqlWithoutSelect
      case extractColumns sqlWithoutSelect of
        Right columns -> do
          let sqlAfterFrom = removeBeforeFrom sqlWithoutSelect
          case extractTableNames sqlAfterFrom of
            Right tableNames -> do
              case hasWhere of
                False -> return $ "Does not have WHERE"
                True -> do
                  let result = removeBeforeWhere sqlAfterFrom
                  return $ removeTrailingSemicolon result
            -- let parsedStatement = Select columns tableNames hasWhere
            -- let result = if hasWhere then removeBeforeWhere sqlAfterFrom else "This can be executed"
            -- return $ show parsedStatement
            -- return result
            Left errorMessage -> Left errorMessage
        Left errorMessage -> Left errorMessage
    Left errorMessage -> Left errorMessage

deleteParser :: String -> Either ErrorMessage ParsedStatement
deleteParser sql =
  case containsDelete sql of
    True -> do
      let sqlWithoutDelete = dropWord sql
      case containsFrom sqlWithoutDelete of
        True -> do
          let sqlWithoutFrom = dropWord sqlWithoutDelete
          case extractTableName sqlWithoutFrom of
            Right tableName -> do
              let sqlWithoutTableName = dropWord sqlWithoutFrom
              case containsWhere sqlWithoutTableName of
                True -> do
                  let sqlWithoutWhere = dropWord sqlWithoutTableName
                  let condition = removeTrailingSemicolon sqlWithoutWhere
                  return $ Delete tableName condition
                False -> return $ Delete tableName ""
            Left errorMessage -> Left errorMessage
        False -> Left "Error: DELETE statement does not contain 'from'"
    False -> Left "Error: SQL statement does not contain 'delete'"

deleteFromDataFrame :: ParsedStatement -> DataFrame -> Either ErrorMessage DataFrame
deleteFromDataFrame (Delete tableName condition) (DataFrame existingColumns existingRows) =
  case extractConditionParts condition of
    (column, value) -> do
      let columnIndex = getColumnIndex (DataFrame existingColumns []) column
      case columnIndex of
        Just index -> do
          let filteredRows = filter (\row -> not (checkCondition row index value)) existingRows
              updatedDataFrame = DataFrame existingColumns filteredRows
          -- Use unsafePerformIO to perform the IO action inside the Either monad
          let result = unsafePerformIO $ do
                encodeDataFrame updatedDataFrame tableName
                return $ Right updatedDataFrame
          seq result (return updatedDataFrame)
        Nothing -> Left "Error: Column not found in the DataFrame"
  where
    checkCondition :: Row -> Int -> String -> Bool
    checkCondition row columnIndex value =
      case (row !! columnIndex, value) of
        (StringValue str, _) -> map toLower str == map toLower value
        (IntegerValue int, _) -> show int == value
        (BoolValue bool, "true") -> bool
        (BoolValue bool, "false") -> not bool
        _ -> False

containsSet :: String -> Bool
containsSet input = case words (map toLower input) of
  ("set" : _) -> True
  _ -> False

parseUpdatePairs :: [String] -> [(ColumnName, InsertValues)]
parseUpdatePairs [] = []
parseUpdatePairs (column : "=" : value : rest) =
  (map toLower column, value) : parseUpdatePairs rest
parseUpdatePairs _ = error "Invalid format in the SET clause"

updateParser :: String -> Either ErrorMessage ParsedStatement
updateParser sql =
  case containsUpdate sql of
    True -> do
      let sqlWithoutUpdate = dropWord sql
      case extractTableName sqlWithoutUpdate of
        Right tableName -> do
          let sqlWithoutTableName = dropWord sqlWithoutUpdate
          case "set" `elem` words (map toLower sqlWithoutTableName) of
            True -> do
              let sqlWithoutSet = dropWord sqlWithoutTableName
              let (updates, rest) = break (== "where") (words (map toLower sqlWithoutSet))
              case updates of
                [] -> Left "Error: No updates found in the SET clause"
                _ -> do
                  let parsedUpdates = parseUpdatePairs updates
                  case rest of
                    ("where" : condition) -> do
                      let parsedCondition = unwords condition
                      Right (Update tableName parsedUpdates parsedCondition)
                    _ -> Left "Error: Missing WHERE clause in UPDATE statement"
            False -> Left "Error: Missing SET clause in UPDATE statement"
        Left errorMessage -> Left errorMessage
    False -> Left "Error: SQL statement does not contain 'update'"

insertParser :: String -> Either ErrorMessage ParsedStatement
insertParser sql =
  case containsInsert sql of
    True -> do
      let sqlWithoutInsert = dropWord sql
      case containsInto sqlWithoutInsert of
        True -> do
          let sqlWithoutInto = dropWord sqlWithoutInsert
          case extractTableName sqlWithoutInto of
            Right tableName -> do
              let sqlWithoutTableName = dropWord sqlWithoutInto
              case containsOpeningBracket sqlWithoutTableName of
                True -> do
                  let sqlWithoutOB = dropChar sqlWithoutTableName
                  case extractColumnNamesUntilClosingParenthesis sqlWithoutOB of
                    Right columns -> do
                      -- let parsedStatement = Insert tableName columns
                      let sqlWithoutColumnNames = getSubstringAfterLastClosingParen sqlWithoutOB
                      case containsValues sqlWithoutColumnNames of
                        True -> do
                          let sqlWithoutValues = dropWord sqlWithoutColumnNames
                          case containsOpeningBracket sqlWithoutValues of
                            True -> do
                              let sqlWithoutSecondOB = dropChar sqlWithoutValues
                              case extractColumnNamesUntilClosingParenthesis sqlWithoutSecondOB of
                                Right values -> do
                                  let parsedStatement = Insert tableName columns values
                                  Right parsedStatement
                                Left errorMessage -> Left errorMessage
                            False -> Left "Error: Missing opening brace"
                        False -> Left "Error: SQL statement does not contain 'values'"
                    Left errorMessage -> Left errorMessage
                False -> Left "Error: Missing opening brace"
            Left errorMessage -> Left errorMessage
        False -> Left "Error: SQL statement does not contain 'into'"
    False -> Left "Error: SQL statement does not contain 'insert'"

insertToDataFrame :: ParsedStatement -> DataFrame -> Either ErrorMessage DataFrame
insertToDataFrame (Insert tableName columns values) (DataFrame existingColumns existingRows) =
  if length columns /= length values
    then Left "Number of columns doesn't match the number of values."
    else
      if not (allColumnsExist columns existingColumns)
        then Left "One or more columns do not exist in the DataFrame."
        else do
          let updatedDataFrame = DataFrame existingColumns (existingRows ++ [map parseValue (zip columns values)])
          -- Use unsafePerformIO to perform the IO action inside the Either monad
          let result = unsafePerformIO $ encodeDataFrame updatedDataFrame tableName
          seq result (return updatedDataFrame)
  where
    allColumnsExist :: [ColumnName] -> [Column] -> Bool
    allColumnsExist cols dfColumns = all (\col -> col `elem` map columnName dfColumns) cols

    parseValue :: (ColumnName, String) -> Value
    parseValue (col, val) = case getColumnByName col existingColumns of
      Just (Column _ colType) -> parseTypedValue colType val
      Nothing -> StringValue val

    parseTypedValue :: ColumnType -> String -> Value
    parseTypedValue IntegerType val = case reads val of
      [(intVal, "")] -> IntegerValue intVal
      _ -> StringValue val
    parseTypedValue StringType val = StringValue val
    parseTypedValue BoolType val = case map toLower val of
      "true" -> BoolValue True
      "false" -> BoolValue False
      _ -> StringValue val

    getColumnByName :: ColumnName -> [Column] -> Maybe Column
    getColumnByName name cols = find (\(Column n _) -> map toLower n == map toLower name) cols

    columnName :: Column -> ColumnName
    columnName (Column name _) = name

updateDataFrame :: ParsedStatement -> DataFrame -> Either ErrorMessage DataFrame
updateDataFrame (Update tableName updates condition) (DataFrame existingColumns existingRows) =
  case extractConditionParts condition of
    (column, value) -> do
      let columnIndex = getColumnIndex (DataFrame existingColumns []) column
      case columnIndex of
        Just index -> do
          let updatedRows = map (updateRow updates index value) existingRows
              updatedDataFrame = DataFrame existingColumns updatedRows
          -- Use unsafePerformIO to perform the IO action inside the Either monad
          let result = unsafePerformIO $ do
                encodeDataFrame updatedDataFrame tableName
                return $ Right updatedDataFrame
          seq result (return updatedDataFrame)
        Nothing -> Left "Error: Column not found in the DataFrame"
  where
    updateRow :: [(ColumnName, UpdateValues)] -> Int -> String -> Row -> Row
    updateRow updates index value row =
      let updatedRow = foldl (\acc (col, newVal) -> updateCell col newVal acc) row updates
       in if checkCondition updatedRow index value then updatedRow else row

    updateCell :: ColumnName -> UpdateValues -> Row -> Row
    updateCell col newVal row =
      case getColumnIndex (DataFrame existingColumns []) col of
        Just colIndex ->
          let parsedValue = parseUpdateValue colIndex newVal
           in case parsedValue of
                Just updatedValue -> replaceAtIndex colIndex updatedValue row
                Nothing -> row
        Nothing -> row

    parseUpdateValue :: Int -> UpdateValues -> Maybe Value
    parseUpdateValue index newVal =
      case getColumnByIndex (DataFrame existingColumns []) index of
        Just (Column _ colType) -> Just (parseTypedValue colType newVal)
        Nothing -> Nothing

    parseTypedValue :: ColumnType -> UpdateValues -> Value
    parseTypedValue IntegerType newVal = case reads newVal of
      [(intVal, "")] -> IntegerValue intVal
      _ -> StringValue newVal
    parseTypedValue StringType newVal = StringValue newVal
    parseTypedValue BoolType newVal = case map toLower newVal of
      "true" -> BoolValue True
      "false" -> BoolValue False
      _ -> StringValue newVal

    getColumnByIndex :: DataFrame -> Int -> Maybe Column
    getColumnByIndex (DataFrame columns _) index =
      if index >= 0 && index < length columns
        then Just (columns !! index)
        else Nothing

    checkCondition :: Row -> Int -> String -> Bool
    checkCondition row columnIndex conditionValue =
      case (row !! columnIndex, conditionValue) of
        (StringValue str, _) -> map toLower str == map toLower conditionValue
        (IntegerValue int, _) -> show int == conditionValue
        (BoolValue bool, "true") -> bool
        (BoolValue bool, "false") -> not bool
        _ -> False

    replaceAtIndex :: Int -> a -> [a] -> [a]
    replaceAtIndex n newVal xs = take n xs ++ [newVal] ++ drop (n + 1) xs

    getColumnIndex :: DataFrame -> ColumnName -> Maybe Int
    getColumnIndex (DataFrame columns _) columnName =
      elemIndex columnName (map (\(Column name _) -> name) columns)
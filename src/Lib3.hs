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
import Data.Aeson (FromJSON, eitherDecode, encode)
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
import Data.Yaml (FromJSON, ToJSON)
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

data ParsedStatement
  = ParsedStatement
  | Select [ColumnName] [TableName] ContainsWhere Condition
  | Insert TableName [ColumnName]
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

containsInsert :: String -> Bool
containsInsert input = case words (map toLower input) of
  ("insert" : _) -> True
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

-- output :: IO ()
-- output = BLC.writeFile "output.json" (encode tableEmployees)

readDataFrameFromJSON :: FilePath -> Either String DataFrame
readDataFrameFromJSON filePath =
  unsafePerformIO $ do
    let fullPath = "src/db/" ++ filePath ++ ".json"
    content <- BLC.readFile fullPath
    return $ eitherDecode content

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
  dataFrames <- mapM (\tableName -> readDataFrameFromJSON tableName) tableNames
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

insertParseHelper :: String -> Either ErrorMessage (String, String)
insertParseHelper sql =
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
                      let parsedStatement = Insert tableName columns
                      let sqlWithoutColumnNames = getSubstringAfterLastClosingParen sqlWithoutOB
                      case containsValues sqlWithoutColumnNames of
                        True -> do
                          let sqlWithoutValues = dropWord sqlWithoutColumnNames
                          Right (show parsedStatement, sqlWithoutValues)
                        False -> Left "Error: SQL statement does not contain 'values'"
                    Left errorMessage -> Left errorMessage
                False -> Left "Error: Missing opening brace"
            Left errorMessage -> Left errorMessage
        False -> Left "Error: SQL statement does not contain 'into'"
    False -> Left "Error: SQL statement does not contain 'insert'"

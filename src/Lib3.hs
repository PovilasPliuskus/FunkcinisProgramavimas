{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Control.Monad (foldM)
import Control.Monad.Free (Free (..), liftF)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Char (isSpace, toLower)
import Data.Maybe
import Data.Time (UTCTime)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row (..), Value (..))
import InMemoryTables (TableName, database)

-- type TableName = String

type ContainsWhere = Bool

type FileContent = String

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

type ColumnName = String

data ParsedStatement
  = ParsedStatement
  | Select [ColumnName] [TableName] ContainsWhere
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
executeSql sql = do
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
                True -> return $ Left "DEBUG: Where was found"
                False ->
                  case createDataFrame (Select columns tableNames hasWhere) of
                    Right dataFrame -> return $ Right dataFrame
                    Left errorMessage -> return $ Left errorMessage
            Left errorMessage -> return $ Left errorMessage
        Left errorMessage -> return $ Left errorMessage
    Left errorMessage -> return $ Left errorMessage

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
removeBeforeWhere input = case break (== "where") (words (map toLower input)) of
  (_, []) -> input
  (_, rest) -> unwords rest

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

createDataFrame :: ParsedStatement -> Either ErrorMessage DataFrame
createDataFrame (Select columns tableNames whereBool) = do
  dataFrames <- mapM (\tableName -> findTableByName InMemoryTables.database tableName) tableNames
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
          combinedRows = [row1 ++ row2 | row1 <- rows1, row2 <- rows2]
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
                True -> return $ "Has Where"
            -- let parsedStatement = Select columns tableNames hasWhere
            -- let result = if hasWhere then removeBeforeWhere sqlAfterFrom else "This can be executed"
            -- return $ show parsedStatement
            -- return result
            Left errorMessage -> Left errorMessage
        Left errorMessage -> Left errorMessage
    Left errorMessage -> Left errorMessage
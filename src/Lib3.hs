{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Control.Monad.Free (Free (..), liftF)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Char (isSpace, toLower)
import Data.Time (UTCTime)
import DataFrame (ColumnType (BoolType), DataFrame)
import InMemoryTables (TableName, database)

-- type TableName = String

type FileContent = String

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

type ColumnName = String

data ParsedStatement
  = ParsedStatement
  | Select [ColumnName] [TableName]
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
      let sqlWithoutSelect = removeSelect sql
      case extractColumns sqlWithoutSelect of
        Right columns -> do
          let sqlAfterFrom = removeBeforeFrom sqlWithoutSelect
          case extractTableNames sqlAfterFrom of
            Right tableNames -> do
              let parsedStatement = Select columns tableNames
              case findTableByName InMemoryTables.database "employees" of
                Just dataFrame -> return $ Right dataFrame
                Nothing -> return $ Left "Error: Table not found"
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

findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName ((tableName, dataFrame) : database) givenName
  | map toLower tableName == map toLower givenName = Just dataFrame
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
-- createDataFrame (Select columns tableNames) =

helperFunction :: String -> Either ErrorMessage String
helperFunction sql = do
  case parseSelect sql of
    Right tableName -> do
      let sqlWithoutSelect = removeSelect sql
      case extractColumns sqlWithoutSelect of
        Right columns -> do
          let sqlAfterFrom = removeBeforeFrom sqlWithoutSelect
          case extractTableNames sqlAfterFrom of
            Right tableNames -> do
              let parsedStatement = Select columns tableNames
              return $ show parsedStatement
            -- return $ sqlAfterFrom
            Left errorMessage -> Left errorMessage
        Left errorMessage -> Left errorMessage
    Left errorMessage -> Left errorMessage
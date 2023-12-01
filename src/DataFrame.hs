{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import Data.Time (TimeZone, UTCTime)
import Data.Yaml (FromJSON, ToJSON, encode)
import GHC.Generics (Generic)

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  -- /| TimestampType TimeZone
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data Column = Column String ColumnType
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  -- /| TimestampValue UTCTime
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

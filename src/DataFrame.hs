module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import Data.Time (TimeZone, UTCTime)

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  | TimestampType TimeZone
  deriving (Show, Eq)

data Column = Column String ColumnType
  deriving (Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  | TimestampValue UTCTime
  deriving (Show, Eq)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq)

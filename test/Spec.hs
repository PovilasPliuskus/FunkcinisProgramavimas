import Control.Monad.Free (Free (..), liftF)
import Data.Either
import Data.Either (Either (Right))
import Data.IORef
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Lib3 (ParsedStatement (Delete, Update), deleteFromDataFrame, updateDataFrame)
import Lib3 qualified
import Test.Hspec
import Text.ParserCombinators.ReadPrec (step)

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  describe "Lib2.parseStatement" $ do
    it "parses show tables statement" $ do
      Lib2.parseStatement "SHOW TABLES" `shouldSatisfy` isRight
    it "parses show tables statement (case insensitivity involved)" $ do
      Lib2.parseStatement "ShoW TabLEs" `shouldSatisfy` isRight
    it "parses show table statement" $ do
      Lib2.parseStatement "SHOW TABLE flags" `shouldSatisfy` isRight
    it "parses show table statement (case insensitivity involved)" $ do
      Lib2.parseStatement "SHoW TaBLe flags" `shouldSatisfy` isRight
    it "parses a simple select statement with an asterisk" $ do
      Lib2.parseStatement "SELECT * FROM employees" `shouldSatisfy` isRight
    it "parses a simple select statement with distinct columns" $ do
      Lib2.parseStatement "SELECT name, id FROM employees" `shouldSatisfy` isRight
    it "parses MIN function" $ do
      Lib2.parseStatement "SELECT MIN(id) FROM employees" `shouldSatisfy` isRight
    it "parses AVG function" $ do
      Lib2.parseStatement "SELECT AVG(name) FROM employees" `shouldSatisfy` isRight
    it "parses WHERE AND function" $ do
      Lib2.parseStatement "SELECT id, name FROM employees WHERE name = Ed AND id = 2" `shouldSatisfy` isRight
    it "parses WHERE BOOL function" $ do
      Lib2.parseStatement "SELECT flag, value FROM flags WHERE value = TRUE" `shouldSatisfy` isRight
  describe "Lib2.executeStatement" $ do
    it "executes SHOW TABLES statement" $ do
      case Lib2.parseStatement "SHOW TABLES" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTablesTest
    it "executes SHOW TABLE employees statement" $ do
      case Lib2.parseStatement "SHOW TABLE employees" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTableEmployeesTest
    it "executes a simple select statement with distinct columns" $ do
      case Lib2.parseStatement "SELECT name, surname FROM employees" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right distinctSelectTableTest
    it "executes MIN function" $ do
      case Lib2.parseStatement "SELECT min(id) FROM employees" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right minTableTest
    it "executes AVG function" $ do
      case Lib2.parseStatement "SELECT avg(id) FROM employees" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right avgTableTest
    it "executes WHERE BOOL function" $ do
      case Lib2.parseStatement "SELECT flag, value FROM flags WHERE value = TRUE" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right whereBoolTableTest
    it "executes WHERE AND function" $ do
      case Lib2.parseStatement "SELECT id, name, surname FROM employees WHERE name = Ed AND surname = Dl" of
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right whereAndTableTest
  describe "Lib3.insertParser" $ do
    it "parses a valid INSERT statement with values" $ do
      let sql = "insert into employees (id, name, surname) values (3, 'Jonas', 'Jonaitis')"
      Lib3.insertParser sql `shouldBe` Right (Lib3.Insert "employees" ["id", "name", "surname"] ["3", "'Jonas'", "'Jonaitis'"])

    it "handles missing 'insert'" $ do
      let sql = "into employees (id, name, surname) values (3, 'Jonas', 'Jonaitis')"
      Lib3.insertParser sql `shouldBe` Left "Error: SQL statement does not contain 'insert'"

    it "handles missing 'into'" $ do
      let sql = "insert employees (id, name, surname) values (3, 'Jonas', 'Jonaitis')"
      Lib3.insertParser sql `shouldBe` Left "Error: SQL statement does not contain 'into'"

    it "handles missing opening brace" $ do
      let sql = "insert into employees id, name, surname) values (3, 'Jonas', 'Jonaitis')"
      Lib3.insertParser sql `shouldBe` Left "Error: Missing opening brace"

  describe "Lib3.updateParser" $ do
    it "parses a valid UPDATE statement with SET and WHERE clauses" $ do
      let sql = "update employees set name = 'John' where id = 1;"
      Lib3.updateParser sql `shouldBe` Right (Lib3.Update "employees" [("name", "'john'")] "id = 1;")

    it "handles missing 'update'" $ do
      let sql = "employees set name = 'John' where id = 1;"
      Lib3.updateParser sql `shouldBe` Left "Error: SQL statement does not contain 'update'"

    it "handles missing SET clause" $ do
      let sql = "update employees where id = 1;"
      Lib3.updateParser sql `shouldBe` Left "Error: Missing SET clause in UPDATE statement"

    it "handles missing WHERE clause" $ do
      let sql = "update employees set name = 'John';"
      Lib3.updateParser sql `shouldBe` Left "Error: Missing WHERE clause in UPDATE statement"

  describe "Lib3.deleteParser" $ do
    it "parses a valid DELETE statement with WHERE clause" $ do
      let sql = "delete from employees where id = 1;"
      Lib3.deleteParser sql `shouldBe` Right (Lib3.Delete "employees" "id = 1")

    it "handles missing 'delete'" $ do
      let sql = "from employees where id = 1;"
      Lib3.deleteParser sql `shouldBe` Left "Error: SQL statement does not contain 'delete'"

    it "handles missing FROM clause" $ do
      let sql = "delete employees where id = 1;"
      Lib3.deleteParser sql `shouldBe` Left "Error: DELETE statement does not contain 'from'"

    it "handles missing WHERE clause" $ do
      let sql = "delete from employees;"
      Lib3.deleteParser sql `shouldBe` Right (Lib3.Delete "employees;" "")

    it "handles missing trailing semicolon in WHERE clause" $ do
      let sql = "delete from employees where id = 1"
      Lib3.deleteParser sql `shouldBe` Right (Lib3.Delete "employees" "id = 1")

  describe "Lib3.executeSql" $ do
    it "Selects column with WHERE criteria" $ do
      db <- setupDB
      df <- runExecuteIO db (Lib3.executeSql "SELECT id from employees WHERE id = 1;")
      df `shouldBe` Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
    it "Selects all columns without WHERE criteria" $ do
      db <- setupDB
      df <- runExecuteIO db (Lib3.executeSql "SELECT id, name, surname FROM employees;")
      df `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
  describe "Lib3.insertToDataFrame" $ do
    it "Inserts values into the DataFrame correctly" $ do
      let initialDataFrame = initialTableEmployees
          result = insertToDataFrame (Insert "employees" ["id", "name", "surname"] ["3", "Jo", "Ka"]) initialDataFrame
          expectedDataFrame = firstInsertTableEmployees
      result `shouldBe` Right expectedDataFrame
    it "Finds the invalid number of Columns" $ do
      let initialDataFrame = initialTableEmployees
          result = insertToDataFrame (Insert "employees" ["id", "name", "surname"] ["3", "Jo"]) initialDataFrame
      result `shouldSatisfy` isLeft
  describe "Lib3.updateDataFrame" $ do
    it "Updates a value in the DataFrame correctly" $ do
      let initialDataFrame = initialTableEmployees
          result = updateDataFrame (Update "employees" [("surname", "Do")] "id = 1") initialDataFrame
      result `shouldBe` Right firstUpdateTableEmployees
    it "Updates several values in the DataFrame correctly" $ do
      let initialDataFrame = initialTableEmployees
          result = updateDataFrame (Update "employees" [("surname", "Do"), ("name", "Di")] "id = 1") initialDataFrame
      result `shouldBe` Right secondUpdateTableEmployees
  describe "Lib3.deleteFromDataFrame" $ do
    it "Deletes a value from the DataFrame correctly" $ do
      let initialDataFrame = initialTableEmployees
          result = deleteFromDataFrame (Delete "employees" "name = Ed") initialDataFrame
      result `shouldBe` Right firstDeleteTableEmployees
    it "Deletes multiple values from the DataFrame correctly" $ do
      let initialDataFrame = repetitiveTableEmployees
          result = deleteFromDataFrame (Delete "employees" "surname = Ka") initialDataFrame
      result `shouldBe` Right initialTableEmployees

initialTableEmployees :: DataFrame
initialTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
      [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
    ]

repetitiveTableEmployees :: DataFrame
repetitiveTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
      [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
      [IntegerValue 3, StringValue "Jo", StringValue "Ka"],
      [IntegerValue 4, StringValue "Pe", StringValue "Ka"]
    ]

firstInsertTableEmployees :: DataFrame
firstInsertTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
      [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
      [IntegerValue 3, StringValue "Jo", StringValue "Ka"]
    ]

firstUpdateTableEmployees :: DataFrame
firstUpdateTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Do"],
      [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
    ]

secondUpdateTableEmployees :: DataFrame
secondUpdateTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Di", StringValue "Do"],
      [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
    ]

firstDeleteTableEmployees :: DataFrame
firstDeleteTableEmployees =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Po"]
    ]

showTablesTest :: DataFrame
showTablesTest =
  DataFrame
    [Column "Table Name" StringType]
    [ [StringValue "employees"],
      [StringValue "invalid1"],
      [StringValue "invalid2"],
      [StringValue "long_strings"],
      [StringValue "flags"]
    ]

showTableEmployeesTest :: DataFrame
showTableEmployeesTest =
  DataFrame
    [Column "Tables" StringType]
    [ [StringValue "id"],
      [StringValue "name"],
      [StringValue "surname"]
    ]

distinctSelectTableTest :: DataFrame
distinctSelectTableTest =
  DataFrame
    [Column "name" StringType, Column "surname" StringType]
    [ [StringValue "Vi", StringValue "Po"],
      [StringValue "Ed", StringValue "Dl"]
    ]

minTableTest :: DataFrame
minTableTest =
  DataFrame
    [Column "min(id)" IntegerType]
    [ [IntegerValue 1]
    ]

avgTableTest :: DataFrame
avgTableTest =
  DataFrame
    [Column "avg(id)" StringType]
    [ [StringValue "1.5"]
    ]

whereBoolTableTest :: DataFrame
whereBoolTableTest =
  DataFrame
    [Column "flag" StringType, Column "value" BoolType]
    [ [StringValue "a", BoolValue True],
      [StringValue "b", BoolValue True]
    ]

whereAndTableTest :: DataFrame
whereAndTableTest =
  DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
    ]

type Database = [(String, IORef String)]

setupDB :: IO Main.Database
setupDB = do
  employees <- newIORef "employees: [[id, IntegerType, name, StringType, surname, StringType], [[contents: 1, tag: IntegerValue, contents: Vi, tag: StringValue, contents: Po, tag: StringValue], [contents: 2, tag: IntegerValue, contents: Ed, tag: StringValue, contents: Dl, tag: StringValue]]]"
  flags <- newIORef "flags: [[id, IntegerType, flag, StringType, value, BoolType], [[contents: 1, tag: IntegerValue, contents: a, tag: StringValue, contents: true, tag: BoolValue], [contents: 1, tag: IntegerValue, contents: b, tag: StringValue, contents: true, tag: BoolValue], [contents: 2, tag: IntegerValue, contents: b, tag: StringValue, contents: null, tag: NullValue], [contents: 2, tag: IntegerValue, contents: b, tag: StringValue, contents: false, tag: BoolValue]]]"
  return [("employees", employees), ("flags", flags)]

runExecuteIO :: Main.Database -> Lib3.Execution a -> IO a
runExecuteIO _ (Pure a) = return a
runExecuteIO db (Free step) = do
  next <- runStep step
  runExecuteIO db next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.LoadFile tableName next) = do
      case Prelude.lookup tableName db of
        Just result -> do
          content <- readIORef result
          return content >>= return . next
        Nothing -> do
          putStrLn $ "\n Table not found"
          return "" >>= return . next

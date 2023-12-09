-- main.hs

module Main (main) where

import Control.Monad.Free (Free (..))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Functor ((<&>))
import Data.List qualified as L
import Data.Time (UTCTime, getCurrentTime)
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names =
        [ "select",
          "*",
          "from",
          "show",
          "table",
          "tables",
          "insert",
          "into",
          "values",
          "set",
          "update",
          "delete",
          "load"
        ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s c
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> String -> IO (Either String String)
    cmd' s command = do
      case words command of
        ("load" : tableName : _) -> do
          content <- readFile ("src/db/" ++ tableName ++ ".yaml")
          return $ Right content
        _ -> do
          df <- runExecuteIO $ Lib3.executeSql command
          return $ Lib1.renderDataFrameAsTable s <$> df

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
  next <- runStep step
  runExecuteIO next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadFile tableName next) = do
      content <- readFile ("src/db/" ++ tableName ++ ".yaml")
      return $ next content

main :: IO ()
main =
  evalRepl (const $ pure "Man patinka Haskell'is> ") cmd [] Nothing Nothing (Word completer) ini final

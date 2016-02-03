module Client
(
client
)
where

import System.IO
import Control.Distributed.Process hiding (proxy)
import Control.Concurrent (threadDelay)
import System.FilePath (takeFileName)
import System.Directory (createDirectoryIfMissing)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary(encode,decode)
import Control.Monad (forever,void)
import Network

import ClientAPI {-(listFilesReq,writeFileReq,readFileReq, shutdownReq)-}
import Messages

-- Example client
client :: ProcessId -> Process ()
client pid = do
  liftIO $ putStrLn "Wait for input"
  input <- liftIO $ getLine --parse some input

  void $ case words input of
          ["show"] -> do
            fsimage <- listFilesReq pid
            showFSImage fsimage
          ["write",localFile,remotePath] -> writeFileReq pid localFile remotePath >> liftIO (putStrLn "read file") --Write a file onto the network
          ["read",path] -> do
              mfdata <- readFileReq pid path --Retrieve the file
              writeToDisk path mfdata --Write file to diskk
          ["quit"] -> liftIO $ (putStrLn "Closing program...") >> threadDelay 2000000 --Print a message and after 2 seconds quit
          ["shutdown"] -> liftIO (putStrLn "Shutting down Network...") >> shutdownReq pid
          _ -> liftIO $ (putStrLn "Input was not a valid command.")

  client pid


--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FilePath] -> Process ()
showFSImage fsimage = mapM_ (\x -> liftIO $ putStrLn $ "  " ++ x) fsimage

writeToDisk :: FilePath -> Maybe FileData -> Process ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> liftIO $ putStrLn "Could not find file on network"
  Just fdata -> do
      liftIO $ createDirectoryIfMissing False "./local"
      liftIO $ B.writeFile ("./local/" ++ takeFileName fpath) fdata

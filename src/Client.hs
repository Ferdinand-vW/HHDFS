module Client
(
client
)
where

import System.IO
import Control.Concurrent (threadDelay)
import System.FilePath (takeFileName)
import System.Directory (createDirectoryIfMissing)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary(encode,decode)

import ClientAPI {-(listFilesReq,writeFileReq,readFileReq, shutdownReq)-}
import Messages

-- Example client
client :: Host -> Handle -> IO ()
client host h = do
  input <- getLine --parse some input
  case words input of
    ["show"] -> do
      fsimage <- listFilesReq h
      client host h
    ["write",localFile,remotePath] -> writeFileReq host h localFile remotePath >> client host h --Write a file onto the network
    ["read",path] -> do
        mfdata <- readFileReq host h path --Retrieve the file
        writeToDisk path mfdata --Write file to disk
        client host h --Read a file from the network
    ["quit"] -> putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
    ["shutdown"] -> (putStrLn "Shutting down Network...") >> shutdownReq h
    _ -> (putStrLn "Input was not a valid command.") >> client host h


--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FilePath] -> IO ()
showFSImage fsimage = mapM_ (\x -> putStrLn $ "  " ++ x) fsimage

writeToDisk :: FilePath -> Maybe FileData -> IO ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> putStrLn "Could not find file on network"
  Just fdata -> do
      createDirectoryIfMissing False "./local"
      B.writeFile ("./local/" ++ takeFileName fpath) fdata

module Client
(
client
)
where

import Control.Distributed.Process
import Control.Concurrent
import System.FilePath (takeFileName)
import System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import qualified Data.ByteString.Lazy.Char8 as B
import ClientAPI (listFilesReq,writeFileReq,readFileReq)
import Messages

-- Example client
client :: ProcessId -> Process ()
client pid = do
  input <- liftIO $ getLine --parse some input
  case words input of
    ["show"] -> do
      fsimage <- listFilesReq pid --get the filenames from the namenode
      showFSImage fsimage
      client pid
    ["write",path] -> writeFileReq pid path >> client pid --Write a file onto the network
    ["read",path] -> do
        mfdata <- readFileReq pid path --Retrieve the file
        writeToDisk path mfdata --Write file to disk
        client pid --Read a file from the network
    ["quit"] -> liftIO $ putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
    _ -> liftIO (putStrLn "Input was not a valid command.") >> client pid
    

--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FilePath] -> Process ()
showFSImage fsimage = do
  mapM_ (\x -> liftIO . putStrLn $ "  " ++ x) fsimage
  
writeToDisk :: FilePath -> Maybe FileData -> Process ()
writeToDisk fpath mfdata = do
    case mfdata of
        Nothing -> liftIO $ putStrLn "Could not find file on network"
        Just fdata -> do
            let fname = takeFileName fpath
            liftIO $ createDirectoryIfMissing False "./local"
            liftIO $ B.writeFile ("./local/" ++ fname) fdata
    

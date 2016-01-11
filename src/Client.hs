module Client
(
client
)
where

import Control.Distributed.Process
import Control.Concurrent
import qualified Data.ByteString.Lazy.Char8 as B
import ClientAPI (listFilesReq,writeFileReq,readFileReq)
import Messages

-- Example client
client :: ProcessId -> Process ()
client pid = do
  input <- liftIO $ getLine --parse some input
  case words input of
    ["show"] -> do
      listFilesReq pid --get the filenames from the namenode
      client pid
    ["write",path] -> writeFileReq pid path >> client pid --Write a file onto the network
    ["read",name] -> readFileReq pid (B.pack name) >> client pid --Read a file from the network
    ["quit"] -> liftIO $ putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
    _ -> liftIO (putStrLn "Input was not a valid command.") >> client pid

module Main where

import System.Environment (getArgs)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process
import qualified Data.ByteString.Lazy.Char8 as B

import NodeInitialization (setupNameNode, setupNode)
import DataNode (dataNode)
import Messages (FileName)
import Client

main :: IO ()
main = do
  (mode : args) <- getArgs

  --address should be of the form: host:port:0
  case mode of
    "namenode" ->
      case args of
        [port] ->      setupNameNode undefined "127.0.0.1" port
        [host,port] -> setupNameNode undefined host port
    "datanode" ->
      case args of
        [port,addr] ->      setupNode dataNode "127.0.0.1" port addr
        [host,port,addr] -> setupNode dataNode host port addr
    "client" ->
      case args of
        [port,addr] ->      setupNode client "127.0.0.1" port addr
        [host,port,addr] -> setupNode client host port addr

-- Example client
client :: ProcessId -> Process ()
client pid = do
  input <- liftIO $ getLine --parse some input
  case words input of
    ["show"] -> do --Client wants to see all the files stored on the server
      fsimage <- listFilesReq pid --get the filenames from the namenode
      showFSImage fsimage --Visually show the filenames
      client pid
    ["write",path] -> writeFileReq pid path >> client pid --Write a file onto the network
    ["read",name] -> readFileReq pid (B.pack name) >> client pid --Read a file from the network
    ["quit"] -> liftIO $ putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
    _ -> liftIO (putStrLn "Input was not a valid command.") >> client pid

--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FileName] -> Process ()
showFSImage fsimage = do
  mapM_ (\x -> liftIO . putStrLn $ "  " ++ (B.unpack x)) fsimage

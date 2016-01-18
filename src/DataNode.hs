module DataNode where

import Control.Concurrent.STM
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import qualified Data.ByteString.Char8 as B
import System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import Control.Monad (when, forever)

import Messages

dataNode :: ProcessId -> Process ()
dataNode nnid = do
  pid <- getSelfPid
  send nnid $ HandShake pid

  liftIO $ createDirectoryIfMissing False "./data"
  handleMessages nnid pid


handleMessages :: ProcessId -> ProcessId -> Process ()
handleMessages nnid myid = do
  blocksT <- liftIO $ newTVarIO []
  forever $ do
    msg <- expect :: Process CDNReq
    case msg of
      CDNRep bid pids -> do
        file <- liftIO $ B.readFile (getFileName bid)
        mapM_ (\x -> send x (CDNWrite bid file)) pids
      CDNRead bid sendPort -> do
        file <- liftIO $ B.readFile (getFileName bid)
        sendChan sendPort file
      CDNWrite bid file -> do
        liftIO $ B.writeFile (getFileName bid) file
        liftIO $ atomically $ modifyTVar blocksT $ \xs -> bid : xs
        blocks <- liftIO $ readTVarIO blocksT
        send nnid (BlockReport myid blocks)
      CDNDelete bid -> liftIO $ do
        let fileName = getFileName bid
        fileExists <- doesFileExist fileName
        when fileExists $ removeFile (getFileName bid)
        liftIO $ atomically $ modifyTVar blocksT $ \xs -> filter (/=bid) xs
      

getFileName :: BlockId -> FilePath
getFileName bid = "./data/" ++ show bid ++ ".dat"

module DataNode where

import Control.Concurrent.STM
import Control.Distributed.Process

import qualified Data.ByteString.Char8 as B
import System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import Control.Monad (when, forever, unless)
import Data.Binary

import Messages

dnDataDir, dnConfDir, dnConfFile :: String
dnDataDir = "./data/"
dnConfDir = "./dn_config/"
dnConfFile  = dnConfDir ++ "datanode.conf"

readDataNodeId :: IO DataNodeId
readDataNodeId = decodeFile dnConfFile

writeDataNodeId :: DataNodeId -> IO ()
writeDataNodeId = encodeFile dnConfFile

verifyConfig :: ProcessId -> Process ()
verifyConfig nnid = do
  fileExist <- liftIO $ doesFileExist dnConfFile
  unless fileExist $ do
    (sendPort, receivePort) <- newChan -- Ask NameNode for an Id if we dont have one already
    send nnid $ WhoAmI sendPort
    res <- receiveChan receivePort -- Recieve the ID from the namenode and store it locally
    liftIO $ writeDataNodeId res

dataNode :: ProcessId -> Process ()
dataNode nnid = do

  pid <- getSelfPid

  liftIO $ createDirectoryIfMissing False dnDataDir
  liftIO $ createDirectoryIfMissing False dnConfDir

  verifyConfig nnid

  dnId <- liftIO readDataNodeId

  send nnid $ HandShake pid dnId
  handleMessages nnid dnId

handleMessages :: ProcessId -> DataNodeId -> Process ()
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
getFileName bid = dnDataDir ++ show bid ++ ".dat"

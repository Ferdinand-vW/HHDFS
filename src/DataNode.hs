module DataNode where

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import qualified Data.ByteString.Lazy as B
import System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import Control.Monad (when, forever, unless)
import Data.Binary

import Messages

dnFataDir = "./data/"
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

  liftIO $ createDirectoryIfMissing False dnFataDir
  liftIO $ createDirectoryIfMissing False dnConfDir

  verifyConfig nnid

  dnId <- liftIO readDataNodeId

  send nnid $ HandShake pid dnId
  handleMessages


handleMessages :: Process ()
handleMessages = forever $ do
  msg <- expect
  case msg of
    CDNRead bid sendPort -> do
      file <- liftIO $ B.readFile (getFileName bid)
      sendChan sendPort file
    CDNWrite bid file ->
      liftIO $ B.writeFile (getFileName bid) file
    CDNDelete bid -> liftIO $ do
      let fileName = getFileName bid
      fileExists <- doesFileExist fileName
      when fileExists $ removeFile (getFileName bid)

getFileName :: BlockId -> FilePath
getFileName bid = dnFataDir ++ show bid ++ ".dat"

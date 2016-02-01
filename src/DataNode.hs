module DataNode where

import Control.Concurrent(threadDelay)
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
dnBlockFile = dnConfDir ++ "datanode.blocks"

readDataNodeId :: IO DataNodeId
readDataNodeId = decodeFile dnConfFile

writeDataNodeId :: DataNodeId -> IO ()
writeDataNodeId = encodeFile dnConfFile

readDataNodeBlocks :: IO [BlockId]
readDataNodeBlocks = decodeFile dnBlockFile

writeDataNodeBlocks :: [BlockId] -> IO ()
writeDataNodeBlocks = encodeFile dnBlockFile

verifyConfig :: ProcessId -> Process ()
verifyConfig nnid = do
  fileExist <- liftIO $ doesFileExist dnConfFile
  unless fileExist $ do
    (sendPort, receivePort) <- newChan -- Ask NameNode for an Id if we dont have one already
    send nnid $ WhoAmI sendPort
    res <- receiveChan receivePort -- Recieve the ID from the namenode and store it locally
    liftIO $ writeDataNodeId res

verifyBlocks :: Process [BlockId]
verifyBlocks = do
  fileExist <- liftIO $ doesFileExist dnBlockFile
  unless fileExist $ do
    liftIO $ writeDataNodeBlocks []
  liftIO $ readDataNodeBlocks

dataNode :: ProcessId -> Process ()
dataNode nnid = do

  pid <- getSelfPid

  liftIO $ createDirectoryIfMissing False dnDataDir
  liftIO $ createDirectoryIfMissing False dnConfDir

  verifyConfig nnid
  bids <- verifyBlocks

  dnId <- liftIO readDataNodeId
  tvarbids <- liftIO $ newTVarIO bids

  send nnid $ HandShake pid dnId bids

  --Spawn a local process, which every 2 seconds sends a blockreport to the namenode
  spawnLocal $ sendBlockReports nnid dnId tvarbids
  --Spawn a local process, which writes the blockIds that this datanode holds to file
  --after every added or deleted blockId
  spawnLocal $ writeBlockReports tvarbids
  handleMessages nnid dnId tvarbids

handleMessages :: ProcessId -> DataNodeId -> TVar [BlockId] -> Process ()
handleMessages nnid myid tvarbids = do
  forever $ do
    msg <- expect :: Process CDNReq
    say $ ("received " ++ (show msg))
    case msg of
      CDNRep bid pids -> do
        file <- liftIO $ B.readFile (getFileName bid)
        mapM_ (\x -> send x (CDNWrite bid file)) pids
      CDNRead bid sendPort -> do
        file <- liftIO $ B.readFile (getFileName bid)
        sendChan sendPort file
      CDNWrite bid file -> do
        liftIO $ B.writeFile (getFileName bid) file
        liftIO $ atomically $ modifyTVar tvarbids $ \xs -> bid : xs
      CDNDelete bid -> liftIO $ do
        let fileName = getFileName bid
        fileExists <- doesFileExist fileName
        when fileExists $ removeFile (getFileName bid)
        liftIO $ atomically $ modifyTVar tvarbids $ \xs -> filter (/=bid) xs

sendBlockReports :: ProcessId -> DataNodeId -> TVar [BlockId] -> Process ()
sendBlockReports nnid myid tvarbids = do
    bids <- liftIO $ readTVarIO tvarbids
    loop bids
  where
    loop oldbids = do
      newbids <- liftIO $ readNewBlockIds tvarbids oldbids --Blocking call, waits for blockIds to be changed
      send nnid (BlockReport myid newbids)
      liftIO $ threadDelay 2000000 --Send a blockreport at most every 2 seconds
      loop newbids

writeBlockReports :: TVar [BlockId] -> Process ()
writeBlockReports tvarbids = do
    bids <- liftIO $ readTVarIO tvarbids
    liftIO $ loop bids
  where
    loop oldbids = do
      newbids <- readNewBlockIds tvarbids oldbids
      writeDataNodeBlocks newbids --Write the blockIds to file
      loop newbids

readNewBlockIds :: TVar [BlockId] -> [BlockId] -> IO [BlockId]
readNewBlockIds tvarbids oldbids = atomically $ do
    newbids <- readTVar tvarbids
    if newbids /= oldbids
        then return newbids
        else retry


getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"

{-# LANGUAGE RecordWildCards #-}

module DataNode where

import           Control.Concurrent(threadDelay)
import           Control.Concurrent.STM
import           Control.Distributed.Process
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import           System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import           Control.Monad (when, forever, unless, join)
import           Data.Binary
import           Data.Typeable


import Messages

data DataNode = DataNode {
    dnId :: DataNodeId
  , blockIds :: TVar [BlockId]
  , procChan :: TChan (Process ())
}

dnDataDir, dnConfDir, dnConfFile :: String
dnDataDir = "./data/"
dnConfDir = "./dn_config/"
dnConfFile  = dnConfDir ++ "datanode.conf"
dnBlockFile = dnConfDir ++ "datanode.blocks"

dataNode :: Port -> ProcessId -> Process ()
dataNode port nnid = do

  pid <- getSelfPid

  -- Check that the needed folders exist and create them if they dont
  liftIO $ createDirectoryIfMissing False dnDataDir
  liftIO $ createDirectoryIfMissing False dnConfDir

  -- Check we have an available id to read or obtain one
  verifyConfig nnid

  -- Read existing blocks and id
  bids <- liftIO readBlocks
  dnId <- liftIO readDataNodeId

  pChan <- liftIO newTChanIO
  sendChan <- liftIO newTChanIO
  tvarbids <- liftIO $ newTVarIO bids

  -- Setup a port for the proxy
  let port' = show $ 1 + read port

  -- Initialize DataNode
  let dn = DataNode { dnId=dnId , blockIds=tvarbids, procChan=pChan}

  -- spawn the main dataNode process
  pid' <- spawnLocal $ handleMessages nnid dn
  send nnid $ HandShake pid' dnId bids port'

  --Spawn a local process, which on changes sends a blockreport to the namenode
  spawnLocal $ sendBlockReports nnid dn
  --Spawn a local process, which writes the blockIds that this datanode holds to file
  --after every added or deleted blockId
  spawnLocal $ writeBlockReports dn

   -- spawn local proxy to execute process and IO actions
  spawnLocal $ spawnProcListener dn

  handleProxyMessages nnid dn

handleMessages :: ProcessId -> DataNode -> Process ()
handleMessages nnid dn@DataNode{..} = forever $ do
  msg <- expect :: Process IntraNetwork
  --liftIO $ putStrLn "received some message"
  spawnLocal $
    case msg of
      Repl bid pids -> do
        liftIO $ putStrLn $ "received repl " ++ show pids
        --liftIO $ putStrLn $ "file name " ++ (getFileName bid)
        file <- liftIO $ L.readFile (getFileName bid)
        --liftIO $ putStrLn $ " gets here"
        unless (null pids) ( do
          --liftIO $ putStrLn $ "sending message " ++ show (head pids)
          liftIO $ atomically $ do
            sendSTM dn (head pids) (WriteFile bid file (tail pids))
          )
      WriteFile bid fdata pids -> do
        --liftIO $ putStrLn "received write request from datanode"
        liftIO $ B.writeFile (getFileName bid) (L.toStrict fdata)
        bids <- liftIO $ atomically $ readTVar blockIds
        --liftIO $ putStrLn $ show bids
        liftIO $ atomically $ modifyTVar blockIds $ \xs -> bid : xs
        unless (null pids) (
          liftIO $ atomically $ do
            sendSTM dn (head pids) (WriteFile bid fdata (tail pids))
          )


handleProxyMessages :: ProcessId -> DataNode -> Process ()
handleProxyMessages nnid dn@DataNode{..} =
  forever $ do
    msg <- expect :: Process ProxyToDataNode
    spawnLocal $
      case msg of
        CDNWriteP bid -> do
          liftIO $ putStrLn "updating blockids"
          liftIO $ atomically $ modifyTVar blockIds $ \xs -> bid : xs
        CDNDeleteP bid -> do
          let fileName = getFileName bid
          fileExists <- liftIO $ doesFileExist fileName
          when fileExists (
            liftIO $ atomically $ do
              writeIOChan dn (liftIO $ removeFile (getFileName bid))
              modifyTVar blockIds $ \xs -> filter (/=bid) xs
            )

-- We send block reports as soon as we detect a change in the datanode blockIds
sendBlockReports :: ProcessId -> DataNode -> Process ()
sendBlockReports nnid dn@DataNode{..} = forever $ do
    bids <- liftIO $ readTVarIO blockIds
    checkStatus bids
  where
    checkStatus oldbids = liftIO $ atomically $ do
      newbids <- readNewBlockIds dn oldbids --Blocking call, waits for blockIds to be changed
      sendSTM dn nnid (BlockReport dnId newbids)

-- We persit the blockIds every time we add a new one
writeBlockReports :: DataNode -> Process ()
writeBlockReports dn@DataNode{..} = forever $ liftIO $ atomically $ do
    bids <- readTVar blockIds
    newbids <- readNewBlockIds dn bids
    writeIOChan dn $ liftIO $ writeDataNodeBlocks newbids

-- Retries untill the block ids have changed
readNewBlockIds :: DataNode -> [BlockId] -> STM [BlockId]
readNewBlockIds dn@DataNode{..} oldbids = do
    newbids <- readTVar blockIds
    if newbids /= oldbids
        then return newbids
        else retry

-- Utility 
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

readBlocks :: IO [BlockId]
readBlocks = do
  fileExist <- doesFileExist dnBlockFile
  unless fileExist $ writeDataNodeBlocks []
  readDataNodeBlocks

getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"

spawnProcListener :: DataNode -> Process ()
spawnProcListener DataNode{..} = forever $ join $ liftIO $ atomically $ readTChan procChan

sendSTM :: (Typeable a, Binary a) => DataNode -> ProcessId -> a -> STM ()
sendSTM DataNode{..} pid msg = writeTChan procChan (send pid msg)

sendChanSTM :: (Typeable a, Binary a) => DataNode -> SendPort a -> a -> STM ()
sendChanSTM DataNode{..} chan msg = writeTChan procChan (sendChan chan msg)

writeIOChan :: DataNode -> Process () -> STM ()
writeIOChan DataNode{..} = writeTChan procChan

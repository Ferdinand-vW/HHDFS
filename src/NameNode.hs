{-# LANGUAGE RecordWildCards#-}

module NameNode where

import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            Control.Concurrent
import            System.FilePath (takeFileName, isValid)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (forM, forever, unless)
import            Text.Printf
import            Data.Char (ord)
import            Data.Maybe
import            Data.Binary
import            System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)


import            DataNode
import            Messages
import            NodeInitialization

type FsImage = Map FilePath Position
type DataNodeIdPidMap = Map DataNodeId ProcessId

data NameNode = NameNode
  { dataNodes :: [DataNodeId]
  , fsImage :: FsImage
  , dnIdPidMap :: DataNodeIdPidMap
  }

nnConfDir, nnDataDir, fsImageFile, dnMapFile :: String
nnConfDir = "./nn_conf/"
nnDataDir = "./nn_data/"
fsImageFile = nnDataDir ++ "fsImage.fs"
dnMapFile = nnDataDir ++ "dn_map.map"

flushFsImage :: FsImage -> IO ()
flushFsImage = encodeFile fsImageFile

readFsImage :: IO FsImage
readFsImage = do
  fileExist <- liftIO $ doesFileExist fsImageFile
  unless fileExist $ flushFsImage M.empty
  decodeFile fsImageFile

flushDnMap :: DataNodeIdPidMap -> IO ()
flushDnMap = encodeFile dnMapFile

readDnMap :: IO DataNodeIdPidMap
readDnMap = do
  fileExist <- liftIO $ doesFileExist dnMapFile
  unless fileExist $ liftIO $ flushDnMap M.empty
  decodeFile dnMapFile


nameNode :: Process ()
nameNode = do
  liftIO $ createDirectoryIfMissing False nnDataDir
  dnMap <- liftIO readDnMap
  fsImg <- liftIO readFsImage
  loop (NameNode [] fsImg dnMap)
  where
    loop nnode = receiveWait
      [ match $ \clientReq -> do
          newNnode <- handleClients nnode clientReq
          loop newNnode
      , match $ \handShake -> do
           newNnode <- handleDataNodes nnode handShake
           loop newNnode
      ]

handleDataNodes :: NameNode -> HandShake -> Process NameNode
handleDataNodes nameNode@NameNode{..} (HandShake pid dnId) = do
  let newMap = M.insert dnId pid dnIdPidMap
  liftIO $ flushDnMap newMap
  return $ nameNode { dataNodes = dnId:dataNodes, dnIdPidMap = newMap }

handleDataNodes nameNode@NameNode{..} (WhoAmI chan) = do
  sendChan chan $ nextDnId (M.keys dnIdPidMap)
  return nameNode
  where
    nextDnId [] = 0
    nextDnId a = maximum a + 1

handleClients :: NameNode -> ClientReq -> Process NameNode
handleClients nameNode@NameNode{..} (Write fp chan) =
  if not $ isValid fp
  then do
    sendChan chan (Left InvalidPathError)
    return nameNode
  else do
    let
      dnodeId = toDnId dataNodes (takeFileName fp) -- pick a data node where to store the file
      positions = M.elems fsImage -- grab the list of positions
      nextFreeBlockId = nextBidFor dnodeId positions -- calculate the next free block id for that data node
      dnodePid = fromMaybe (error "This should never happen.") $ M.lookup dnodeId dnIdPidMap
      response = (dnodePid, nextFreeBlockId)
      newfsImage = M.insert fp (dnodeId, nextFreeBlockId) fsImage
    sendChan chan (Right response)
    liftIO $ flushFsImage newfsImage
    return $ nameNode { fsImage = newfsImage }

handleClients nameNode@NameNode{..} (Read  fp chan) = do
  let res = M.lookup fp fsImage
  case res of
    Nothing -> do
      sendChan chan (Left FileNotFound)
      return nameNode
    Just (dnodeId, blockId) ->
      case M.lookup dnodeId dnIdPidMap of
        Nothing -> do
          sendChan chan (Left InconsistentNetwork)
          return nameNode
        Just dnodePid -> do
          sendChan chan (Right (dnodePid, blockId))
          return nameNode

handleClients nameNode@NameNode{..} (ListFiles chan) = do
  sendChan chan (M.keys fsImage)
  return nameNode

handleClients nameNode@NameNode{..} Shutdown =
  mapM_ (`kill` "User shutdown") (mapMaybe toPid dataNodes) >> liftIO (threadDelay 20000) >> terminate
    where
      toPid dnodeId = M.lookup dnodeId dnIdPidMap


-- Another naive implementation to find the next free block id given a datanode
-- This should be changed to something more robust and performant
nextBidFor :: DataNodeId -> [Position] -> BlockId
nextBidFor pid []        = 0
nextBidFor pid positions = maximum (map toBid positions) + 1
  where
    toBid (nnodePid, blockId) = if nnodePid == pid then blockId else 0

-- For the time being we can pick the dataNote where to store a file with this
-- naive technique.
toDnId :: [DataNodeId] -> String -> DataNodeId
toDnId pids s = pids !! (ord (head s) `mod` length pids)

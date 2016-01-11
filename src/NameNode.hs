{-# LANGUAGE RecordWildCards#-}

module NameNode where

import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            System.FilePath (takeFileName)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (forM, forever)
import            Text.Printf
import            Data.Char (ord)

import            DataNode
import            Messages (ClientReq(..), BlockId, Position, HandShake(..), CDNReq)
import            NodeInitialization

type FsImage = Map FilePath Position
type DataNodeMap = Map ProcessId (SendPort CDNReq)

data NameNode = NameNode
  { dataNodes :: [ProcessId]
  , fsImage :: FsImage
  }

flushFsImage :: FsImage -> IO ()
flushFsImage fs = writeFile "./fsImage/fsImage.fs" (show fs)

-- no Read instance for ProcessId, we should just use a better
-- serialization/deserialization method
-- readFsImage :: IO FsImage
-- readFsImage = do
--   s <- readFile "./fsImage/fsImage.fs"
--   let fsImg = read s :: FsImage
--   return fsImg

nameNode :: Process ()
nameNode = loop (NameNode [] M.empty)
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
handleDataNodes nameNode@NameNode{..} (HandShake pid) = return $
  nameNode { dataNodes = pid:dataNodes }

handleClients :: NameNode -> ClientReq -> Process NameNode
handleClients nameNode@NameNode{..} (Write fp chan) = do
  let
    dnodePid = toPid dataNodes (takeFileName fp) -- pick a data node where to store the file
    positions = M.elems fsImage -- grab the list of positions
    nextFreeBlockId = nextBidFor dnodePid positions -- calculate the next free block id for that data node
    newPosition = (dnodePid, nextFreeBlockId)
  sendChan chan newPosition
  return $ nameNode { fsImage = M.insert fp newPosition fsImage }

handleClients nameNode@NameNode{..} (Read  fp chan) = do
  let res = M.lookup fp fsImage
  sendChan chan res
  return nameNode

handleClients nameNode@NameNode{..} (ListFiles chan) = do
  sendChan chan (M.keys fsImage)
  return nameNode

-- Another naive implementation to find the next free block id given a datanode
-- This should be changed to something more robust and performant
nextBidFor :: ProcessId -> [Position] -> BlockId
nextBidFor pid []        = 0
nextBidFor pid positions = maximum (map toBid positions) + 1
  where
    toBid (nnodePid, blockId) = if nnodePid == pid then blockId else 0

-- For the time being we can pick the dataNote where to store a file with this
-- naive technique.
toPid :: [ProcessId] -> String -> ProcessId
toPid pids s = pids !! (ord (head s) `mod` length pids)

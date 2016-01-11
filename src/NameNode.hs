{-# LANGUAGE RecordWildCards#-}

import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            System.FilePath (takeFileName)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (forM, forever)
import            Text.Printf
import            Data.Char (ord)

import            DataNode
import            Messages (ClientReq(..), BlockId, Position)
import            NodeInitialization

type FileName = String
type FsImage = Map FileName Position

data NameNode = NameNode
  { dataNodes :: [ProcessId]
  , fsImage :: FsImage
  }

main = initNameNode nameNode

flushFsImage :: FsImage -> IO ()
flushFsImage fs = writeFile "./fsImage/fsImage.fs" (show fs)

-- no Read instance for ProcessId, we should just use a better
-- serialization/deserialization method
-- readFsImage :: IO FsImage
-- readFsImage = do
--   s <- readFile "./fsImage/fsImage.fs"
--   let fsImg = read s :: FsImage
--   return fsImg

nameNode :: Process ProcessId
nameNode = loop (NameNode [] M.empty)
  where
    loop nnode = receiveWait
      [ match $ \clientReq -> loop $ handleClients nnode req
      , match $ \handShake -> loop $ handleDataNodes nnode handShake
      ]

handleDataNodes :: NameNode -> HandShake -> Process NameNode
handleDataNodes nnode@NameNode{..} (HandShake pid) = return $ NameNode pid:dataNodes fsImage

handleClients :: NameNode -> ClientReq -> Process NameNode
handleClients nameNode@NameNode{..} (Write fp chan) = do
  let
    dnodePid = toPid dataNodes (takeFileName fp)
    positions = M.elems fsImage
    nextFreeBlockId = nextBidFor dnodePid positions
  sendChan chan (dnodePid, nextFreeBlockId)
  return nameNode

handleClients nameNode@NameNode{..} (Read  fp chan) = do
  let res = M.lookup fp fsImage
  sendChan chan res
  return nameNode

-- Another naive implementation to find the next free block id given a datanode
-- This should be changed to something more robust and performant
nextBidFor :: ProcessId -> [Position] -> BlockId
nextBidFor pid positions = maximum (map toBid positions) + 1
  where
    toBid (nnodePid, blockId) = if nnodePid == pid then blockId else 0

-- For the time being we can pick the dataNote where to store a file with this
-- naive technique.
toPid :: [ProcessId] -> String -> ProcessId
toPid pids s = pids !! (ord (head s) `mod` length pids)

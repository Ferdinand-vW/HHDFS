{-# LANGUAGE TemplateHaskell, RecordWildCards#-}

module NameNode where

import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            DataNode
import            Control.Monad (forM)
import            Text.Printf
import            Messages (ClientReq(..), BlockId)

type Position = (ProcessId, BlockId)
type FileName = String
type FsImage = Map FileName Position

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

-- do we really need a hearthbeat? what if the namenode simply starts up the datanods withMonitor

initializeDataNodes :: [NodeId] -> Process ProcessId
initializeDataNodes nids = do
  ps <- forM nids $ \nid -> do
    say $ printf "starting DataNode %s" (show nid)
    spawn nid $(mkStaticClosure 'dataNode)

  spawnLocal $ clientHandler (NameNode ps M.empty)

handleClientReq :: ClientReq -> Process ()
handleClientReq (Write fp chan) = undefined
handleClientReq (Read  fp) = undefined

clientHandler :: NameNode -> Process ()
clientHandler nameNode = loop nameNode
  where
    loop nameNode@NameNode{..} = do
      req <- expect :: Process ClientReq
      handleClientReq req
      loop nameNode

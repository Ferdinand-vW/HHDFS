{-# LANGUAGE RecordWildCards#-}

module NameNode where

import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            Control.Concurrent
import            System.FilePath (takeFileName, isValid)
import            Data.Map (Map)
import qualified  Data.Map as M
import qualified  Data.Set as S
import            Control.Monad (forM, forever)
import            Text.Printf
import            Data.Char (ord)

import            DataNode
import            Messages
import            NodeInitialization

repFactor :: Int
repFactor = 2

type FsImage = Map FilePath BlockId
type BlockMap = Map BlockId (S.Set ProcessId)
type DataNodeMap = Map ProcessId (SendPort CDNReq)

data NameNode = NameNode
  { dataNodes :: [ProcessId]
  , fsImage :: FsImage
  , blockMap :: BlockMap
  , repMap :: BlockMap
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
nameNode = loop (NameNode [] M.empty M.empty M.empty)
  where
    loop nnode = receiveWait
      [ match $ \clientReq -> do
          newNnode <- handleClients nnode clientReq
          loop newNnode
      , match $ \handShake -> do
          newNnode <- handleDataNodes nnode handShake
          loop newNnode
      , match $ \blockreport -> do
          newNnode <- handleBlockReport nnode blockreport
          loop newNnode
      ]

handleDataNodes :: NameNode -> HandShake -> Process NameNode
handleDataNodes nameNode@NameNode{..} (HandShake pid) = return $
  nameNode { dataNodes = pid:dataNodes }

handleClients :: NameNode -> ClientReq -> Process NameNode
handleClients nameNode@NameNode{..} (Write fp chan) =
  if not $ isValid fp
  then do
    sendChan chan (Left InvalidPathError)
    return nameNode
  else do
    let
      dnodePid = toPid dataNodes (takeFileName fp) -- pick a data node where to store the file
      positions = map (\(k,a) -> (head $ S.toList a,k)) (M.toList blockMap) -- grab the list of positions
      nextFreeBlockId = nextBidFor dnodePid positions -- calculate the next free block id for that data node
      newPosition = (dnodePid, nextFreeBlockId)
    sendChan chan (Right newPosition)
    return $ nameNode { fsImage = M.insert fp nextFreeBlockId fsImage
                      , blockMap = M.insert nextFreeBlockId (S.singleton dnodePid) blockMap }

handleClients nameNode@NameNode{..} (Read fp chan) = do
  let res = M.lookup fp fsImage
  case res of
    Nothing -> do
      sendChan chan (Left FileNotFound)
      return nameNode
    Just bid -> do
      --We can either lookup twice or lookup once after a union
      let mpids = M.lookup bid (M.unionWith S.union blockMap repMap)
      case mpids of
        Nothing -> do
          say "Was not able to find any ProcessId's that contain that file."
          sendChan chan (Left FileNotFound)
          return nameNode
        Just pids -> do
          sendChan chan (Right $ (head $ S.toList pids, bid))
          return nameNode

handleClients nameNode@NameNode{..} (ListFiles chan) = do
  sendChan chan (M.keys fsImage)
  return nameNode

handleClients nameNode@NameNode{..} Shutdown =
  mapM_ (`kill` "User shutdown") dataNodes >> liftIO (threadDelay 20000) >> terminate

--Whenever we receive a blockreport from a datanode we will have to update
--the repmap and the blockmap.
handleBlockReport :: NameNode -> BlockReport -> Process NameNode
handleBlockReport nameNode@NameNode{..} (BlockReport pid blocks) = do
  --If a BlockId from the given list of BlockId's exists in the current Map, then we simply
  --add the given ProcessId to the Set of ProcessId's corresponding to that BlockId.
  --For the repmap, if a BlockId doesn't already exist then we ignore it. Whereas
  --we insert a new entry into the blockmap.
  let repmap = foldr (\x y -> if M.member x y
                                then M.adjust (\set -> S.insert pid set) x y
                                else y) repMap blocks
      blockmap  = foldr (\x y -> if M.member x y
                                then M.adjust (\set -> S.insert pid set) x y
                                else M.insert x (S.singleton pid) y) blockMap blocks
      --We are done with incorporating the blockreport into the maps, but we possibly have to move
      --entries from the repmap to the blockmap and vica versa.
      --First we partition both maps on how many ProcessId's contain a BlockId
      (toblmap,repmap') = M.partition (\x -> S.size x >= repFactor + 1) repmap
      (torepmap,blockmap') = M.partition (\x -> S.size x <= repFactor + 1) blockmap
      --Next we union the results
      newBlMap = M.unionWith S.union toblmap blockmap'
      newRepMap = M.unionWith S.union torepmap repmap'
  --Any BlockId's that were moved from the blockmap to the replicationmap have to be replicated.
  --For any BlockId that has to be replicated we select the replication factor - the number of
  --datanodes that already have that BlockId + 1 for the dataNode that sent this blockreport, afterwards
  --we send a Replication request to the `current` dataNode to send that BlockId to the selected dataNodes
  mapM_ (\(k,a) -> send pid $ CDNRep k $
                    selectDataNodes (repFactor - S.size a + 1) pid dataNodes)
                                                        (M.toList torepmap)
  return $ nameNode { blockMap = newBlMap, repMap = newRepMap}

--Simple method for selecting datanodes that we want to replicate to
selectDataNodes :: Int -> ProcessId -> [ProcessId] -> [ProcessId]
selectDataNodes n pid dataNodes = take n $ filter (/= pid) dataNodes

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

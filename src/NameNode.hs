{-# LANGUAGE RecordWildCards, ScopedTypeVariables #-}

module NameNode where

import            Control.Distributed.Process hiding (proxy)
import            Control.Concurrent (threadDelay)
import            Control.Concurrent.STM
import            GHC.Conc
import            System.FilePath (takeFileName, isValid)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (unless, forever, join, void)
import qualified  Data.Set as S
import            Data.Char (ord)

import            Data.Maybe (fromJust, mapMaybe, fromMaybe)
import            Data.Binary (encodeFile, decodeFile, Binary)
import            Data.Typeable
import            System.Directory (doesFileExist, createDirectoryIfMissing)

import            Messages

repFactor :: Int
repFactor = 2

type FsImage = Map FilePath [BlockId]
type BlockMap = Map BlockId (S.Set DataNodeId)
type DataNodeIdPidMap = Map DataNodeId ProcessId


data NameNode = NameNode
  { dataNodes :: TVar [DataNodeId]
  , fsImage :: TVar FsImage
  , dnIdPidMap :: TVar DataNodeIdPidMap
  , blockMap :: TVar BlockMap
  , repMap :: TVar BlockMap
  , proxyChan :: TChan (Process ())
  , ioChan :: TChan (Process ())
  }

nnConfDir, nnDataDir, fsImageFile, dnMapFile, blockMapFile :: String
nnConfDir = "./nn_conf/"
nnDataDir = "./nn_data/"
fsImageFile = nnDataDir ++ "fsImage.fs"
dnMapFile = nnDataDir ++ "dn_map.map"
blockMapFile = nnDataDir ++ "block_map.map"

mkNameNode :: STM NameNode
mkNameNode = do
  oldDnMap <- unsafeIOToSTM readDnMap
  oldFsImg <- unsafeIOToSTM readFsImage

  pChan <- newTChan
  ioChan <- newTChan
  fsImg <- newTVar oldFsImg
  dnMap <- newTVar oldDnMap
  dNodes <- newTVar []
  bMap <- newTVar M.empty
  rMap <- newTVar M.empty

  return NameNode { dataNodes=dNodes
                , fsImage=fsImg
                , dnIdPidMap=dnMap
                , blockMap=bMap
                , repMap=rMap
                , proxyChan=pChan
                , ioChan = ioChan }

nameNode :: Process ()
nameNode = do
  liftIO $ createDirectoryIfMissing False nnDataDir

  nn <- liftIO $ atomically mkNameNode

  spawnLocal (proxy nn) -- spawn local proxy to execute process actions
  ioThread <- spawnLocal (proxyIO nn) -- spawn io proxy to flush data to disk

  link ioThread

  forever $
    receiveWait
      [ match $ \(clientReq :: ClientReq) ->
          void $ spawnLocal (liftIO $ atomically $ handleClients nn clientReq)
      , match $ \(handShake :: HandShake) ->
          liftIO $ atomically $ handleDataNodes nn handShake
      , match $ \(blockreport :: BlockReport) ->
          liftIO $ atomically $ handleBlockReport nn blockreport
      ]


handleDataNodes :: NameNode -> HandShake -> STM ()
handleDataNodes nameNode@NameNode{..} (HandShake pid dnId bids) = do
  idPidMap <- readTVar dnIdPidMap
  bMap <- readTVar blockMap
  dNodes <- readTVar dataNodes
  let newMap = M.insert dnId pid idPidMap
      dnIdSet = S.singleton dnId --Share this set
      bidMap = M.fromList $ map (\x -> (x,dnIdSet)) bids --make tuples of a blockId and the dnIdSet,
                                                         --then use that to construct a Map
      newBlockMap = M.unionWith S.union bMap bidMap --Add all blockId's of the current DataNode to the BlockMap

  writeIOChan nameNode $ liftIO $ flushDnMap newMap
  writeTVar dnIdPidMap newMap
  writeTVar blockMap newBlockMap
  modifyTVar' dataNodes (dnId:)

handleDataNodes nameNode@NameNode{..} (WhoAmI chan) = do
  idPidMap <- readTVar dnIdPidMap
  sendChanSTM nameNode chan $ nextDnId (M.keys idPidMap)
  where
    nextDnId [] = 0
    nextDnId a = maximum a + 1

handleClients :: NameNode -> ClientReq -> STM ()
handleClients nameNode@NameNode{..} (Write fp blockCount chan) = do
  dNodes <- readTVar dataNodes
  idPidMap <- readTVar dnIdPidMap
  bMap <- readTVar blockMap
  if not $ isValid fp
  then sendChanSTM nameNode chan (Left InvalidPathError)
  else do
    let dnodePids = mapMaybe (`M.lookup` idPidMap) dNodes
        selectedDnodes = take blockCount $ map (toPid dnodePids (takeFileName fp)) [0..]
        positions = zip selectedDnodes [(length $ M.keys bMap)..]
        updateFsImg = M.insert fp (map snd positions)

    sendChanSTM nameNode chan (Right positions)
    writeIOChan nameNode $ liftIO $ flushFsImage nameNode
    modifyTVar' fsImage updateFsImg

handleClients nameNode@NameNode{..} (Read fp chan) = do
  fsImg <- readTVar fsImage
  case M.lookup fp fsImg of
    Nothing -> sendChanSTM nameNode chan (Left FileNotFound)
    Just bids -> do
      --We can either lookup twice or lookup once after a union
      --let mpids = M.lookup bid (M.unionWith S.union blockMap repMap)
      bMap <- readTVar blockMap
      rMap <- readTVar repMap
      idPidMap <- readTVar dnIdPidMap

      -- We could insert some retries here instead of the fromJust
      let mpids = map (\bid -> (head $ S.toList $ fromJust $ M.lookup bid $ M.unionWith S.union bMap rMap, bid)) bids
      let res = map (\(dnodeId, bid) -> (fromJust $ M.lookup dnodeId idPidMap, bid)) mpids
      sendChanSTM nameNode chan (Right res)

handleClients nameNode@NameNode{..} (ListFiles chan) = do
  fsImg <- readTVar fsImage
  sendChanSTM nameNode chan (Right $ M.keys fsImg)

handleClients nameNode@NameNode{..} Shutdown = do
  dNodes <- readTVar dataNodes
  idPidMap <- readTVar dnIdPidMap
  mapM_ (\pid -> writeIOChan nameNode (kill pid "User shutdown")) (mapMaybe (`M.lookup` idPidMap) dNodes)
  writeIOChan nameNode terminate


--Whenever we receive a blockreport from a datanode we will have to update
--the repmap and the blockmap.
handleBlockReport :: NameNode -> BlockReport -> STM ()
handleBlockReport nameNode@NameNode{..} (BlockReport dnodeId blocks) = do
  bMap <- readTVar blockMap
  rMap <- readTVar repMap
  idPidMap <- readTVar dnIdPidMap
  dNodes <- readTVar dataNodes
  --First we have to determine whether to add a block to BlockMap or RepMap. If the block
  --already exists in the RepMap, then we add the ProcessId to the corresponding set. If not,
  --we try the same for the BlockMap. If it exists in neither then it has to be a new BlockId
  --and therefore we add it to the BlockMap
  let foldBlocks x (bl,rep) = case (M.member x rep, M.member x bl) of
        (True, _)  -> (bl                                 , M.adjust (S.insert dnodeId) x rep)
        (_, True)  -> (M.adjust (S.insert dnodeId) x bl   , rep                              )
        (_, False) -> (M.insert x (S.singleton dnodeId) bl, rep                              )

      (blockmap,repmap) = foldr foldBlocks (bMap,rMap) blocks
      --We are done with incorporating the blockreport into the maps, but we possibly have to move
      --entries from the repmap to the blockmap and vica versa.
      --First we partition both maps on how many ProcessId's contain a BlockId
      (toblmap,repmap') = M.partition (\x -> S.size x >= repFactor + 1) repmap
      (torepmap,blockmap') = M.partition (\x -> S.size x <= repFactor) blockmap
      --Next we union the results
      newBlMap = M.unionWith S.union toblmap blockmap'
      newRepMap = M.unionWith S.union torepmap repmap'
  --Any BlockId's that were moved from the blockmap to the replicationmap have to be replicated.
  --For any BlockId that has to be replicated we select the replication factor - the number of
  --datanodes that already have that BlockId + 1 for the dataNode that sent this blockreport, afterwards
  --we send a Replication request to the `current` dataNode to send that BlockId to the selected dataNodes
      pid = fromMaybe (error "This should not happen") $ M.lookup dnodeId idPidMap

      --Simple method for selecting datanodes that we want to replicate to
      selectDataNodes :: Int  -> [DataNodeId] -> [ProcessId]
      selectDataNodes n dataNodes = take n $ filter (/= pid) dataNodesPids
        where
          dataNodesPids = mapMaybe (`M.lookup` idPidMap) dataNodes

  mapM_ (\(k,a) -> sendSTM nameNode pid $ CDNRep k $
                    selectDataNodes (repFactor - S.size a + 1) dNodes)
                                                        (M.toList torepmap)
  return ()


------- UTILITY ------


proxy :: NameNode -> Process ()
proxy NameNode{..} = forever $ join $ liftIO $ atomically $ readTChan proxyChan

proxyIO :: NameNode -> Process ()
proxyIO NameNode{..} = forever $ join $ liftIO $ atomically $ readTChan ioChan

sendSTM :: (Typeable a, Binary a) => NameNode -> ProcessId -> a -> STM ()
sendSTM NameNode{..} pid msg = writeTChan proxyChan (send pid msg)

sendChanSTM :: (Typeable a, Binary a) => NameNode -> SendPort a -> a -> STM ()
sendChanSTM NameNode{..} chan msg = writeTChan proxyChan (sendChan chan msg)

writeIOChan :: NameNode -> Process () -> STM ()
writeIOChan NameNode{..} = writeTChan ioChan

-- Another naive implementation to find the next free block id given a datanode
-- This should be changed to something more robust and performant
nextBidFor :: DataNodeId -> [LocalPosition] -> BlockId
nextBidFor _   []        = 0
nextBidFor pid positions = maximum (map toBid positions) + 1
  where
    toBid (nnodePid, blockId) | nnodePid == pid = blockId
                              | otherwise       = 0

-- For the time being we can pick the dataNode where to store a file with this
-- naive technique.
toPid :: [ProcessId] -> String -> Int -> ProcessId
toPid pids s bix = pids !! ((ord (head s) + bix) `mod` length pids)


flushFsImage :: NameNode -> IO ()
flushFsImage NameNode{..} = do
  contents <- atomically $ readTVar fsImage
  liftIO $ encodeFile fsImageFile contents

readFsImage :: IO FsImage
readFsImage = do
  fileExist <- liftIO $ doesFileExist fsImageFile
  unless fileExist $ encodeFile fsImageFile emptyFsImg
  decodeFile fsImageFile
  where
    emptyFsImg :: FsImage
    emptyFsImg = M.empty

flushDnMap :: DataNodeIdPidMap -> IO ()
flushDnMap = encodeFile dnMapFile

readDnMap :: IO DataNodeIdPidMap
readDnMap = do
  fileExist <- liftIO $ doesFileExist dnMapFile
  unless fileExist $ liftIO $ flushDnMap M.empty
  decodeFile dnMapFile

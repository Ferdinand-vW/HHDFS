{-# LANGUAGE RecordWildCards, ScopedTypeVariables #-}

module NameNode where

import            Control.Distributed.Process hiding (proxy)
import            Control.Concurrent (threadDelay)
import            Control.Concurrent.STM
import            Control.Exception
import            GHC.Conc hiding (Shutdown)
import            System.FilePath (takeFileName, isValid)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (unless, forever, join, void, forM)
import qualified  Data.Set as S
import qualified  Data.List as L
import            Data.Char (ord)

import            Data.Maybe (fromJust, mapMaybe, fromMaybe)
import            Data.Binary (encodeFile, decodeFile, Binary)
import            Data.Typeable
import            System.Directory (doesFileExist, createDirectoryIfMissing)
import            System.Random

import            Messages

repFactor :: Int
repFactor = 1

type FsImage = Map FilePath [BlockId]
type BlockMap = Map BlockId (S.Set DataNodeId)
type DataNodeAddressMap = Map DataNodeId Port
type DataNodeIdPidMap = Map DataNodeId ProcessId


data NameNode = NameNode
  { dataNodes :: TVar [DataNodeId]
  , fsImage :: TVar FsImage
  , dnIdPidMap :: TVar DataNodeIdPidMap
  , dnIdAddrMap :: TVar DataNodeAddressMap
  , blockMap :: TVar BlockMap
  , repMap :: TVar BlockMap
  , procChan :: TChan (Process ())
  , blockIdCounter :: TVar Int
  , dataNodeIdCounter :: TVar Int
  , randomVar :: TVar StdGen
  }

nnConfDir, nnDataDir, fsImageFile, dnMapFile, blockMapFile :: String
nnConfDir = "./nn_conf/"
nnDataDir = "./nn_data/"
fsImageFile = nnDataDir ++ "fsImage.fs"
dnMapFile = nnDataDir ++ "dn_map.map"
blockMapFile = nnDataDir ++ "block_map.map"
blockIdFile = nnDataDir ++ "blockId.id"
dataNodeIdFile = nnDataDir ++ "dataNodeId.id"

mkNameNode :: STM NameNode
mkNameNode = do

  -- This is safe to do since we are only reading config files that do not change in this transaction
  oldDnMap <- unsafeIOToSTM readDnMap
  oldFsImg <- unsafeIOToSTM readFsImage
  oldBlockId <- unsafeIOToSTM readBlockId
  oldDataNodeId <- unsafeIOToSTM readDataNodeId

  -- Initialize everything that is needed
  pChan <- newTChan
  fsImg <- newTVar oldFsImg
  dnMap <- newTVar oldDnMap
  addrMap <- newTVar M.empty
  dNodes <- newTVar []
  bMap <- newTVar M.empty
  rMap <- newTVar M.empty
  blockId <- newTVar oldBlockId
  dataNodeId <- newTVar oldDataNodeId
  rVar <- newTVar $ mkStdGen 0

  return NameNode { dataNodes=dNodes
                , fsImage=fsImg
                , dnIdPidMap=dnMap
                , dnIdAddrMap = addrMap
                , blockMap=bMap
                , repMap=rMap
                , procChan=pChan
                , blockIdCounter=blockId
                , dataNodeIdCounter = dataNodeId
                , randomVar=rVar }

nameNode :: Process ()
nameNode = do
  liftIO $ createDirectoryIfMissing False nnDataDir


  nn <- liftIO $ atomically mkNameNode

  -- spawn local proxy to execute process and IO actions
  spawnLocal (spawnProcListener nn)

  forever $
    receiveWait
      [ match $ \(clientReq :: ProxyToNameNode) ->
          void $ spawnLocal (liftIO $ atomically $ handleClients nn clientReq)
      , match $ \(handShake :: HandShake) ->
          liftIO $ atomically $ handleDataNodes nn handShake
      , match $ \(blockreport :: BlockReport) ->
          liftIO $ atomically $ handleBlockReport nn blockreport
      ]

-- A Datanode can send us two kind of messages. In a HandShake the dn tells us
-- his identity. We add the datanode to the dataNodeList and his blocks to the block map
handleDataNodes :: NameNode -> HandShake -> STM ()
handleDataNodes nameNode@NameNode{..} (HandShake pid dnId bids address) = do
  idPidMap <- readTVar dnIdPidMap
  bMap <- readTVar blockMap
  dNodes <- readTVar dataNodes
  dnIdAddrs <- readTVar dnIdAddrMap
  let newMap = M.insert dnId pid idPidMap
      dnIdSet = S.singleton dnId --Share this set
      bidMap = M.fromList $ map (\x -> (x,dnIdSet)) bids --make tuples of a blockId and the dnIdSet,
                                                         --then use that to construct a Map
      newBlockMap = M.unionWith S.union bMap bidMap --Add all blockId's of the current DataNode to the BlockMap
      dnAddressMap = M.insert dnId address dnIdAddrs
  writeIOChan nameNode $ liftIO $ flushDnMap newMap
  writeTVar dnIdPidMap newMap
  writeTVar blockMap newBlockMap
  writeTVar dnIdAddrMap dnAddressMap
  modifyTVar' dataNodes (dnId:)

-- In case a Datanode sends a WhoAmiI we assign a unique id to that datanode and
-- send it back to him to complete initialization
handleDataNodes nameNode@NameNode{..} (WhoAmI chan) = do
  dnIdCounter <- readTVar dataNodeIdCounter
  modifyTVar dataNodeIdCounter (+1)
  writeIOChan nameNode (liftIO $ flushDataNodeId $ dnIdCounter + 1)
  sendChanSTM nameNode chan $ dnIdCounter + 1

-- Handle messages from the namenode proxy
handleClients :: NameNode -> ProxyToNameNode -> STM ()
handleClients nameNode@NameNode{..} (WriteP fp blockCount chan) = do
  dNodes <- readTVar dataNodes
  idPidMap <- readTVar dnIdPidMap
  bMap <- readTVar blockMap
  dnIdAddrs <- readTVar dnIdAddrMap
  blockId <- readTVar blockIdCounter
  if not $ isValid fp
  then sendChanSTM nameNode chan (Left InvalidPathError)
  else do
    let
      dnodePids = mapMaybe (`M.lookup` idPidMap) dNodes
      dnodeAddrs = mapMaybe (`M.lookup` dnIdAddrs) dNodes

    -- We pick some random datanodes to store the blocks we are receiving
    selectedDnodes <- selectRandomDataNodes nameNode blockCount dnodeAddrs

    let
      positions = zip selectedDnodes [(blockId + 1)..]
      updateFsImg = M.insert fp (map snd positions)
      maxBlockId = blockId + length selectedDnodes

    -- We flush the fsImage and the taken blockIds to assure consistency on startup
    writeIOChan nameNode $ liftIO $ flushBlockId maxBlockId
    writeIOChan nameNode $ liftIO $ flushFsImage nameNode

    -- We update the tvars holding the updated data
    writeTVar blockIdCounter maxBlockId
    modifyTVar' fsImage updateFsImg

    -- We send the positions back to the client via the procChanel
    sendChanSTM nameNode chan (Right positions)


handleClients nameNode@NameNode{..} (ReadP fp chan) = do
  fsImg <- readTVar fsImage
  dnIdAddrs <- readTVar dnIdAddrMap
  writeIOChan nameNode $ liftIO $ putStrLn "test"
  case M.lookup fp fsImg of
    Nothing -> sendChanSTM nameNode chan (Left FileNotFound)
    Just bids -> do
      --We can either lookup twice or lookup once after a union
      --let mpids = M.lookup bid (M.unionWith S.union blockMap repMap)
      bMap <- readTVar blockMap
      rMap <- readTVar repMap
      idPidMap <- readTVar dnIdPidMap

      -- We look for the requested blocks in our bmap.
      -- If we dont find them yet, this means they are currently being replicated
      -- so we call retry. As soon as the replication is over this transaction will be rerun
      -- and the blocks will all be found.
      mpids <- forM bids $ pickPids bMap
      let mpidsnbids = zip mpids bids
      let res = map (\(dnodeIds, bid) -> (fromJust $ M.lookup (S.elemAt 0 dnodeIds) dnIdAddrs, bid)) mpidsnbids
      sendChanSTM nameNode chan (Right res)

handleClients nameNode@NameNode{..} (ListFilesP chan) = do
  fsImg <- readTVar fsImage
  sendChanSTM nameNode chan (Right $ M.keys fsImg)


pickPids :: BlockMap -> BlockId -> STM (S.Set DataNodeId)
pickPids bMap bid =
  case M.lookup bid bMap of
    Nothing -> retry
    Just a -> return a


hopefully a = case a of
  Nothing -> retry
  Just a -> return a
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

  writeTVar blockMap newBlMap
  writeTVar repMap newRepMap
  mapM_ (\(k,a) -> do
    let dataNodesPids = filter (/= pid) $ mapMaybe (`M.lookup` idPidMap) dNodes
    dnIds <- selectRandomTakeDataNodes nameNode (repFactor - S.size a + 1) dataNodesPids
    sendSTM nameNode pid $ Repl k dnIds) (M.toList torepmap)

  return ()


------- UTILITY ------


spawnProcListener :: NameNode -> Process ()
spawnProcListener NameNode{..} = forever $ join $ liftIO $ atomically $ readTChan procChan

sendSTM :: (Typeable a, Binary a) => NameNode -> ProcessId -> a -> STM ()
sendSTM NameNode{..} pid msg = writeTChan procChan (send pid msg)

sendChanSTM :: (Typeable a, Binary a) => NameNode -> SendPort a -> a -> STM ()
sendChanSTM NameNode{..} chan msg = writeTChan procChan (sendChan chan msg)

writeIOChan :: NameNode -> Process () -> STM ()
writeIOChan NameNode{..} = writeTChan procChan

selectRandomDataNodes :: (Show a,Eq a) => NameNode -> Int -> [a] -> STM [a]
selectRandomDataNodes nn@NameNode{..} n datanodes = do
 g <- readTVar randomVar
 let (g',r) = randomValues g n datanodes
 writeTVar randomVar g'
 return r


randomValues :: (Show a,Eq a) => StdGen -> Int -> [a] -> (StdGen,[a])
randomValues gen _ [] = (gen,[])
randomValues gen 0 _ = (gen,[])
randomValues gen n xs =
  let (a,g) = randomR (0,length xs - 1) gen
      val  = xs !! a
      (g',ys) = randomValues g (n - 1) xs
  in (g',val:ys)

selectRandomTakeDataNodes :: (Show a,Eq a) => NameNode -> Int -> [a] -> STM [a]
selectRandomTakeDataNodes nn@NameNode{..} n datanodes = do
 g <- readTVar randomVar
 let (g',r) = randomTakeValues g n datanodes
 writeTVar randomVar g'
 return r

randomTakeValues :: (Show a,Eq a) => StdGen -> Int -> [a] -> (StdGen,[a])
randomTakeValues gen _ [] = (gen,[])
randomTakeValues gen 0 _ = (gen,[])
randomTakeValues gen n xs =
  let (a,g) = randomR (0,length xs - 1) gen
      val = xs !! a
      ([x],ys) = L.partition (==val) xs
      (g',zs) = randomTakeValues g (n - 1) ys
  in (g',x:zs)

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

flushBlockId :: Int -> IO ()
flushBlockId = encodeFile blockIdFile

readBlockId :: IO Int
readBlockId = do
  fileExist <- liftIO $ doesFileExist blockIdFile
  unless fileExist $ liftIO $ flushBlockId 0
  decodeFile blockIdFile

flushDataNodeId :: Int -> IO ()
flushDataNodeId = encodeFile dataNodeIdFile

readDataNodeId :: IO Int
readDataNodeId = do
  fileExist <- liftIO $ doesFileExist dataNodeIdFile
  unless fileExist $ liftIO $ flushDataNodeId 0
  decodeFile dataNodeIdFile

{-# LANGUAGE RecordWildCards, ScopedTypeVariables #-}

module NameNode where

import            Control.Distributed.Process hiding (proxy)
import            Control.Concurrent (threadDelay)
import            Control.Concurrent.STM
import            GHC.Conc
import            System.FilePath (takeFileName, isValid)
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (unless, forever, join, void,sequence)
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
  oldDnMap <- unsafeIOToSTM readDnMap
  oldFsImg <- unsafeIOToSTM readFsImage
  oldBlockId <- unsafeIOToSTM readBlockId
  oldDataNodeId <- unsafeIOToSTM readDataNodeId

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

  spawnLocal (spawnProcListener nn) -- spawn local proxy to execute process and IO actions

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
  writeIOChan nameNode $ say $ show (dnId : dNodes)

handleDataNodes nameNode@NameNode{..} (WhoAmI chan) = do
  dnIdCounter <- readTVar dataNodeIdCounter
  modifyTVar dataNodeIdCounter (+1)
  writeIOChan nameNode (liftIO $ flushDataNodeId $ dnIdCounter + 1)
  sendChanSTM nameNode chan $ (dnIdCounter + 1)

handleClients :: NameNode -> ClientReq -> STM ()
handleClients nameNode@NameNode{..} (Write fp blockCount chan) = do
  writeIOChan nameNode $ say "received Write req from client"
  dNodes <- readTVar dataNodes
  idPidMap <- readTVar dnIdPidMap
  bMap <- readTVar blockMap
  blockId <- readTVar blockIdCounter
  if not $ isValid fp
  then sendChanSTM nameNode chan (Left InvalidPathError)
  else do
    let
      dnodePids = mapMaybe (`M.lookup` idPidMap) dNodes

    selectedDnodes <- selectRandomDataNodes nameNode blockCount dnodePids

    let
      positions = zip selectedDnodes [(blockId + 1)..]
      updateFsImg = M.insert fp (map snd positions)
      maxBlockId = blockId + length selectedDnodes

    writeIOChan nameNode $ say $ "response for client " ++ (show positions)
    writeIOChan nameNode $ liftIO $ flushBlockId maxBlockId
    writeIOChan nameNode $ liftIO $ flushFsImage nameNode

    writeTVar blockIdCounter maxBlockId
    modifyTVar' fsImage updateFsImg

    sendChanSTM nameNode chan (Right positions)


handleClients nameNode@NameNode{..} (Read fp chan) = do
  writeIOChan nameNode $ say "received Read req from client"
  fsImg <- readTVar fsImage
  case M.lookup fp fsImg of
    Nothing -> sendChanSTM nameNode chan (Left FileNotFound)
    Just bids -> do
      --We can either lookup twice or lookup once after a union
      --let mpids = M.lookup bid (M.unionWith S.union blockMap repMap)
      bMap <- readTVar blockMap
      rMap <- readTVar repMap
      idPidMap <- readTVar dnIdPidMap
      writeIOChan nameNode $ say $ show bids ++ "    " ++ show bMap ++ "    " ++ show rMap
      return ()
      ---We could insert some retries here instead of the fromJust
      mpids <- mapM (\bid -> do case M.lookup bid $ M.unionWith S.union bMap rMap of
                                                    Just k -> return $ (head $ S.toList k, bid)
                                                    Nothing -> error "test") bids
      let res = map (\(dnodeId, bid) -> (case M.lookup dnodeId idPidMap of
                                            Just k -> k
                                            Nothing -> error "lookup dnId dnIdAddrs", bid)) mpids
      writeIOChan nameNode $ say $ "response for client " ++ (show res)
      sendChanSTM nameNode chan (Right res)


handleClients nameNode@NameNode{..} (ListFiles chan) = do
  writeIOChan nameNode $ say "received Show req from client"
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
  writeIOChan nameNode $ say $ "received blockrep from " ++ (show dnodeId)
  writeIOChan nameNode $ say $ "blocks: " ++ (show blocks)
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

  writeIOChan nameNode $ say $ "new repmap " ++ (show newRepMap)

  writeTVar blockMap newBlMap
  writeTVar repMap newRepMap
  mapM_ (\(k,a) -> do
    let dataNodesPids = filter (/=pid) $ mapMaybe (`M.lookup` idPidMap) dNodes
    dnIds <- selectRandomTakeDataNodes nameNode (repFactor - S.size a + 1) dataNodesPids
    writeIOChan nameNode $ say $ "Chosen dnIds " ++ (show dnIds)
    sendSTM nameNode pid $ CDNRep k dnIds) (M.toList torepmap)

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
      ([x],ys) = case L.partition (==val) xs of
                    ([x],l) -> ([x],l)
                    ps -> error $ "Errrorrr" ++ show ps ++ "  " ++ show val ++ "  " ++ show xs ++ "  " ++ show a
      (g',zs) = randomTakeValues g (n - 1) ys
  in (g',x:zs)

randomValues :: (Show a,Eq a) => StdGen -> Int -> [a] -> (StdGen,[a])
randomValues gen _ [] = (gen,[])
randomValues gen 0 _ = (gen,[])
randomValues gen n xs =
  let (a,g) = randomR (0,length xs - 1) gen
      val  = xs !! a
      (g',ys) = randomValues g (n - 1) xs
  in (g',val:ys)

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



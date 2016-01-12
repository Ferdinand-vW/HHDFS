{-# LANGUAGE RecordWildCards#-}

module NameNode where

import            Control.Distributed.Process
import            Control.Distributed.Process.Closure
import            Control.Concurrent
import            System.FilePath (takeFileName, isValid, splitDirectories, dropFileName)
import            Data.Maybe
import            Data.List as L
import            Data.Map (Map)
import qualified  Data.Map as M
import            Control.Monad (forM, forever)
import            Text.Printf
import            Data.Char (ord)

import            DataNode
import            Messages
import            NodeInitialization

import Debug.Trace

type FsImage = Map Path [File]
type DataNodeMap = Map ProcessId (SendPort CDNReq)

data NameNode = NameNode
  { dataNodes :: [ProcessId]
  , fsImage :: FsImage
  , freeBlockMap :: Map ProcessId Int
  }

data Path = Root | Path :/: String
  deriving (Eq, Show)

instance Ord Path where
  compare Root a = LT
  compare a Root = GT
  compare (p :/: s) (q :/: r) = compare p q

fpToPath :: FilePath -> Path
fpToPath s = foldl (:/:) Root $ drop 1 $ splitDirectories $ dropFileName s

dirToPath :: FilePath -> Path
dirToPath s = foldl (:/:) Root $ drop 1 $ splitDirectories s


toFile :: FilePath -> Position -> File
toFile s p = File { name = takeFileName s, position = p }

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
nameNode = loop (NameNode [] (M.singleton Root []) M.empty)
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
    nextFreeBlockId = fromMaybe 0 $ M.lookup dnodePid freeBlockMap
    newPosition = (dnodePid, nextFreeBlockId)
    currentPath = fpToPath fp
    currentFile = toFile fp newPosition

  sendChan chan (Right newPosition)
  return $ nameNode { fsImage = M.alter (insertFile currentFile) currentPath fsImage
                    , freeBlockMap = M.alter updateBlockId dnodePid freeBlockMap}

handleClients nameNode@NameNode{..} (Read fp chan) = do
  let currentPath = M.lookup (trace (show $ fpToPath fp) fpToPath fp) (trace (show fsImage) fsImage)
  case currentPath of
    Nothing -> do
      sendChan chan (Left DirectoryNotFound)
      return nameNode
    Just path -> do
      let res = findFileWithName (takeFileName fp) path
      case res of
        Nothing -> do
          sendChan chan (Left FileNotFound)
          return nameNode
        Just (File n p) -> do
          sendChan chan (Right p)
          return nameNode

handleClients nameNode@NameNode{..} (ListFiles dir chan) = do
  sendChan chan (fromMaybe [] $ M.lookup (dirToPath dir) fsImage)
  return nameNode

handleClients nameNode@NameNode{..} Shutdown =
  mapM_ (`kill` "User shutdown") dataNodes >> liftIO (threadDelay 20000) >> terminate

updateBlockId :: Maybe Int -> Maybe Int
updateBlockId Nothing  = Just 0
updateBlockId (Just x) = Just $ x + 1

findFileWithName :: FileName -> [File] -> Maybe File
findFileWithName fname = L.find (hasName fname)
  where
    hasName s (File n p) = s == n

insertFile :: File -> Maybe [File] -> Maybe [File]
insertFile file Nothing = Just [file]
insertFile file (Just files) = Just (file : files)


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

{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}

module Messages where

import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary(encode,decode)

type DataNodeId = Int
type BlockId = Int
type Host = String
type Port = String
type LocalPosition = (DataNodeId, BlockId)
type RemotePosition = (ProcessId, BlockId)
type RemoteAddress = (Port,BlockId)

type BlockCount = Int
type FileData = B.ByteString


-- Block size in bytes. For now, very small for testing purposes
blockSize :: Integer
blockSize = 1048576 --1MB

data ClientConnection = Response ProcessId
  deriving (Typeable, Generic)

data HandShake = HandShake
  { dataNodePid    :: ProcessId
  , dataNodeUid    :: Int
  , dataNodeBlocks :: [BlockId]
  , proxyPort      :: Port
  }
  | WhoAmI (SendPort DataNodeId)
  deriving (Typeable, Generic)

data ProxyToNameNode = ListFilesP (SendPort [FilePath])
                    | ReadP FilePath (SendPort (ClientRes [RemoteAddress]))
                    | WriteP FilePath BlockCount (SendPort (ClientRes [RemoteAddress]))
                    | Shutdown
  deriving (Typeable, Generic)

data ClientToNameNode = ListFiles
                      | Read FilePath
                      | Write FilePath BlockCount
  deriving (Typeable, Generic)

data ProxyToClient = FilePaths [FilePath]
                   | ReadAddress (ClientRes [RemoteAddress])
                   | WriteAddress (ClientRes [RemoteAddress])
                   | FileBlock FileData
  deriving (Typeable, Generic)


type ClientRes a = Either ClientError a

data ClientError = InvalidPathError
                 | FileNotFound
                 | InconsistentNetwork
  deriving (Typeable, Generic, Show)

data ProxyToDataNode = CDNReadP BlockId (SendPort FileData)
            | CDNWriteP BlockId FileData
            | CDNDeleteP BlockId
  deriving (Typeable, Generic)

data ClientToDataNode = CDNRead BlockId
                      | CDNWrite BlockId FileData
                      | CDNDelete BlockId
  deriving (Typeable, Generic)

data IntraNetwork = Repl BlockId [ProcessId]
  deriving (Typeable, Generic)

data BlockReport = BlockReport DataNodeId [BlockId]
  deriving (Typeable, Generic)

instance Binary ProxyToNameNode
instance Binary ProxyToClient
instance Binary ProxyToDataNode

instance Binary ClientToNameNode
instance Binary ClientToDataNode

instance Binary IntraNetwork
instance Binary HandShake
instance Binary ClientError
instance Binary BlockReport

toByteString :: Binary a => a -> B.ByteString
toByteString = L.toStrict . encode

fromByteString :: Binary a => B.ByteString -> a
fromByteString = decode . L.fromStrict

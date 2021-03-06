{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}

module Messages where

import           GHC.Generics (Generic)
import           Control.Distributed.Process
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.Binary(decode,encode)
import           Data.Typeable (Typeable)
import           Data.Binary (Binary)
import qualified Data.Set as S

type DataNodeId = Int
type BlockId = Int
type Host = String
type Port = String
type FileName = String
type LocalPosition = (DataNodeId, BlockId)
type RemotePosition = (ProcessId, BlockId)
type RemoteAddress = (Port,BlockId)

type BlockCount = Int
type FileData = L.ByteString


-- Block size in bytes. For now, very small for testing purposes
blockSize :: Integer
blockSize = 1048576 `div` 2 --1/2MB

data ClientConnection = Response ProcessId
  deriving (Typeable, Generic)

data HandShake = HandShake
  { dataNodePid    :: ProcessId
  , dataNodeUid    :: Int
  , dataNodeBlocks :: S.Set BlockId
  , proxyPort      :: Port
  }
  | WhoAmI (SendPort DataNodeId)
  deriving (Typeable, Generic)

data ProxyToNameNode = ListFilesP (SendPort (ClientRes [FilePath]))
                    | ReadP FilePath (SendPort (ClientRes [RemoteAddress]))
                    | WriteP FilePath BlockCount (SendPort (ClientRes [RemoteAddress]))
  deriving (Typeable, Generic)

data ClientToNameNode = ListFiles
                      | Read FilePath
                      | Write FilePath BlockCount
  deriving (Typeable, Generic)

data ProxyToClient = FilePaths (ClientRes [FilePath])
                   | ReadAddress (ClientRes [RemoteAddress])
                   | WriteAddress (ClientRes [RemoteAddress])
                   | OK --Signals client that the proxy is ready for a certain action
  deriving (Typeable, Generic,Show)


type ClientRes a = Either ClientError a

data ClientError = InvalidPathError
                 | FileNotFound
                 | InconsistentNetwork
  deriving (Typeable, Generic, Show)

data ProxyToDataNode = CDNWriteP BlockId
                     | CDNDeleteP BlockId
  deriving (Typeable, Generic, Show)

data ClientToDataNode = CDNRead BlockId
                      | CDNWrite BlockId
                      | CDNDelete BlockId
  deriving (Typeable, Generic, Show)

data IntraNetwork = Repl BlockId [ProcessId]
                  | WriteFile BlockId FileData [ProcessId]
  deriving (Typeable, Generic, Show)

data BlockReport = BlockReport DataNodeId (S.Set BlockId)
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

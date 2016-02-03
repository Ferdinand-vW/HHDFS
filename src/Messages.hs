{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}

module Messages where

import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary(decode,encode)

type DataNodeId = Int
type BlockId = Int
type Host = String
type Port = String
type FileName = String
type LocalPosition = (DataNodeId, BlockId)
type RemotePosition = (ProcessId, BlockId)
type RemoteAddress = (Port,BlockId)

type BlockCount = Int
type FileData = B.ByteString


-- Block size in bytes. For now, very small for testing purposes
blockSize :: Integer
blockSize = 1 * 1048576 --1MB

data ClientConnection = Response ProcessId
  deriving (Typeable, Generic)

data HandShake = HandShake
  { dataNodePid    :: ProcessId
  , dataNodeUid    :: Int
  , dataNodeBlocks :: [BlockId]
  }
  | WhoAmI (SendPort DataNodeId)
  deriving (Typeable, Generic)

data ClientReq = ListFiles (SendPort (ClientRes [FilePath]))
              | Read FilePath (SendPort (ClientRes [RemotePosition]))
              | Write FilePath BlockCount (SendPort (ClientRes [RemotePosition]))
              | Shutdown
  deriving (Typeable, Generic)

{-data ProxyToNameNode = ListFilesP (SendPort (ClientRes [FilePath]))
                    | ReadP FilePath (SendPort (ClientRes [RemoteAddress]))
                    | WriteP FilePath BlockCount (SendPort (ClientRes [RemoteAddress]))
                    | Shutdown
  deriving (Typeable, Generic)-}

{-data ClientToNameNode = ListFiles
                      | Read FilePath
                      | Write FilePath BlockCount
  deriving (Typeable, Generic, Show)-}

{-data ProxyToClient = FilePaths (ClientRes [FilePath])
                   | ReadAddress (ClientRes [RemoteAddress])
                   | WriteAddress (ClientRes [RemoteAddress])
                   | FileBlock FileData
  deriving (Typeable, Generic,Show)-}


type ClientRes a = Either ClientError a

data ClientError = InvalidPathError
                 | FileNotFound
                 | InconsistentNetwork
  deriving (Typeable, Generic, Show)

{-data ProxyToDataNode = CDNWriteP BlockId
                     | CDNDeleteP BlockId
  deriving (Typeable, Generic, Show)-}

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
            | CDNRep BlockId [ProcessId]
  deriving (Typeable, Generic, Show)

{-data IntraNetwork = Repl BlockId [ProcessId]
                  | WriteFile BlockId FileData [ProcessId]
  deriving (Typeable, Generic, Show)-}

data BlockReport = BlockReport DataNodeId [BlockId]
  deriving (Typeable, Generic)

{-instance Binary ProxyToNameNode
instance Binary ProxyToClient
instance Binary ProxyToDataNode-}

{-instance Binary ClientToNameNode
instance Binary ClientToDataNode-}

instance Binary ClientReq
instance Binary CDNReq

{-instance Binary IntraNetwork-}
instance Binary HandShake
instance Binary ClientError
instance Binary BlockReport

{-instance Serialize ProxyToClient
instance Serialize ClientToNameNode
instance Serialize ClientToDataNode
instance Serialize ClientError-}

toByteString :: Binary a => a -> B.ByteString
toByteString = L.toStrict . encode

fromByteString :: Binary a => B.ByteString -> a
fromByteString = decode . L.fromStrict

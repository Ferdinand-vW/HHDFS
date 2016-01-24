{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}

module Messages where

import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Char8 as B

type DataNodeId = Int
type BlockId = Int
type LocalPosition = (DataNodeId, BlockId)
type RemotePosition = (ProcessId, BlockId)
type BlockCount = Int
type FileData = B.ByteString


-- Block size in bytes. For now, very small for testing purposes
blockSize :: Integer
blockSize = 1048576 --1MB

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

type ClientRes a = Either ClientError a

data ClientError = InvalidPathError
                 | FileNotFound
                 | InconsistentNetwork
  deriving (Typeable, Generic, Show)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
            | CDNRep BlockId [ProcessId]
  deriving (Typeable, Generic)

data BlockReport = BlockReport DataNodeId [BlockId]
  deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientError
instance Binary ClientReq
instance Binary CDNReq
instance Binary BlockReport

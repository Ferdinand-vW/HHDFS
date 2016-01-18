{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Lazy as B

type DataNodeId = Int
type BlockId = Int
type Position = (DataNodeId, BlockId)

type FileData = B.ByteString

data HandShake = HandShake
  { dataNodePid :: ProcessId
  , dataNodeUid :: Int
  }
  | WhoAmI (SendPort DataNodeId)
  deriving (Typeable, Generic)

data ClientReq = ListFiles (SendPort [FilePath])
               | Read FilePath (SendPort ClientRes)
               | Write FilePath (SendPort ClientRes)
               | Shutdown
  deriving (Typeable, Generic)

type ClientRes = Either ClientError (ProcessId, BlockId)

data ClientError = InvalidPathError
                 | FileNotFound
                 | InconsistentNetwork
  deriving (Typeable, Generic, Show)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
  deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientError
instance Binary ClientReq
instance Binary CDNReq
